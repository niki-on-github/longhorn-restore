import longhorn
import warnings
import contextlib
import requests
import traceback
import json
import logging
import os
import sys
import time

from urllib3.exceptions import InsecureRequestWarning

VERSION='0.0.1'

old_merge_environment_settings = requests.Session.merge_environment_settings


@contextlib.contextmanager
def no_ssl_verification():
    opened_adapters = set()

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        # Verification happens only once per connection so we need to close
        # all the opened adapters once we're done. Otherwise, the effects of
        # verify=False persist beyond the end of this context manager.
        opened_adapters.add(self.get_adapter(url))

        settings = old_merge_environment_settings(self, url, proxies, stream, verify, cert)
        settings['verify'] = False

        return settings

    requests.Session.merge_environment_settings = merge_environment_settings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = old_merge_environment_settings

        for adapter in opened_adapters:
            try:
                adapter.close()
            except:
                pass


class LonghornClient(longhorn.Client):


    VOLUME_STATE_ATTACHED = "attached"
    VOLUME_STATE_DETACHED = "detached"


    def __init__(self, url: str):
        super().__init__(url=url)
        self.retry_inverval_in_seconds = 1
        self.retry_counts = 120
        self.wait_detached_volumes = {}


    def get_backup_volumes_by_pvc_name(self, pvc_name: str) -> list:
        backup_volumes = self.list_backupVolume()
        result = []
        for backup_volume in backup_volumes:
            try:
                backup_pvc_name = json.loads(backup_volume["labels"]["KubernetesStatus"])["pvcName"]
                if backup_pvc_name == pvc_name:
                    result.append(backup_volume)
            except Exception:
                print(traceback.format_exc())

        return result


    def get_available_backup_volumes_pvc_names(self) -> list:
        backup_volumes = self.list_backupVolume()
        result = []
        for backup_volume in backup_volumes:
            try:
                result.append(json.loads(backup_volume["labels"]["KubernetesStatus"])["pvcName"])
            except Exception:
                print(traceback.format_exc())

        return result


    def wait_for_volume_creation(self, volume_name: str) -> None:
        for _ in range(self.retry_counts):
            volumes = self.list_volume()
            for volume in volumes:
                if volume.name == volume_name:
                    return
            time.sleep(self.retry_inverval_in_seconds)
        raise FileNotFoundError(f"{volume_name} not found")


    def wait_for_volume_status(self, volume_name: str, value: str) -> dict:
        self.wait_for_volume_creation(volume_name)
        for _ in range(self.retry_counts):
            volume = self.by_id_volume(volume_name)
            print(f"Volume {volume_name} state:", volume["state"])
            if volume["state"] == value:
                return volume
            time.sleep(self.retry_inverval_in_seconds)
        raise TimeoutError(f"{volume_name} volume does not satisfy condition {value}")


    def wait_for_volume_detached(self, volume_name: str) -> dict:
        return self.wait_for_volume_status(volume_name, self.VOLUME_STATE_DETACHED)


    def wait_volume_kubernetes_status(self, volume_name: str, expect_ks: dict) -> None:
        for _ in range(self.retry_counts):
            expected = True
            volume = self.by_id_volume(volume_name)
            ks = volume.kubernetesStatus
            ks = json.loads(json.dumps(ks, default=lambda o: o.__dict__))

            for k, v in expect_ks.items():
                print(f"{volume_name} {k}: {ks[k]}")
                if k in ('lastPVCRefAt', 'lastPodRefAt'):
                    if (v != '' and ks[k] == '') or (v == '' and ks[k] != ''):
                        expected = False
                        break
                else:
                    if ks[k] != v:
                        expected = False
                        break
            if expected:
                break

            time.sleep(self.retry_inverval_in_seconds)

        if not expected:
            raise TimeoutError(f"{volume_name} volume does not satisfy condition {expect_ks}")


    def create_pv_for_volume(self, volume, pv_name, fs_type="ext4") -> None:
        volume.pvCreate(pvName=pv_name, fsType=fs_type)

        ks = {
            'pvName': pv_name,
            'pvStatus': 'Available',
            'lastPVCRefAt': '',
            'lastPodRefAt': '',
        }
        self.wait_volume_kubernetes_status(volume.name, ks)


    def create_pvc_for_volume(self, volume, pvc_namespace:str, pvc_name: str) -> None:
        volume.pvcCreate(namespace=pvc_namespace, pvcName=pvc_name)

        ks = {
            'pvStatus': 'Bound',
            'lastPVCRefAt': '',
        }
        self.wait_volume_kubernetes_status(volume.name, ks)


    def create_volume_from_backup(self, pvc_name) -> None:
        backup_volumes = self.get_backup_volumes_by_pvc_name(pvc_name)
        for backup_volume in backup_volumes:
            try:
                pv_name = json.loads(backup_volume["labels"]["KubernetesStatus"])["pvName"]
                pvc_name = json.loads(backup_volume["labels"]["KubernetesStatus"])["pvcName"]
                if self.by_id_volume(id=pv_name):
                    print(f"Volume Handle \"{pv_name}\" ({pvc_name}) exists, skipping")
                    continue
            except Exception:
                print(traceback.format_exc())
                continue

            print(f"Volume Handle \"{pv_name}\" ({pvc_name}) not found, restoring...")
            bv = self.by_id_backupVolume(id=pv_name)
            if bv.lastBackupName:
                last_backup = bv.backupGet(name=bv.lastBackupName)
            else:
                print(f"Backup for \"{pv_name}\" ({pvc_name}) not found, skipping")
                continue

            self.create_volume(name=pv_name, size=bv.size, fromBackup=last_backup.url)
            self.wait_detached_volumes[pvc_name] = {
                    "pv_name": pv_name,
                    "status": json.loads(last_backup.labels.KubernetesStatus)
                }


    def finalize_restored_volume(self, pvc_name, config: dict) -> None:
        if pvc_name not in self.wait_detached_volumes:
            return

        kStatus = self.wait_detached_volumes[pvc_name]["status"]
        pv_name = self.wait_detached_volumes[pvc_name]["pv_name"]
        createPV = config["createPV"] if "createPV" in config else True
        createPVC = config["createPVC"] if "createPVC" in config else True
        groups = config["groups"] if "groups" in config else []
        pvName = config["pvName"] if "pvName" in config else kStatus["pvName"]
        pvcName = config["pvcName"] if "pvcName" in config else kStatus["pvcName"]
        pvcNamespace = config["pvcNamespace"] if "pvcNamespace" in config else kStatus["namespace"]

        volume = self.wait_for_volume_detached(pv_name)
        print(f"Restored volume {pvc_name}")
        if groups:
            for groupName in groups:
                volume.recurringJobAdd(name=groupName, isGroup=True)

        if createPV:
            self.create_pv_for_volume(volume, pvName)
            print(f"Restored PersistentVolume {pvName}")

        if createPVC:
            self.create_pvc_for_volume(volume, pvcNamespace, pvcName)
            print(f"Restored PersistentVolumeClaim {pvcNamespace}/{pvcName}")

        del self.wait_detached_volumes[pvc_name]


def restor_backup():
    client = LonghornClient(url=os.getenv('LONGHORN_URL', 'http://longhorn-frontend.longhorn-system/v1'))
    print('available backup pvc names:', client.get_available_backup_volumes_pvc_names())
    with open(os.getenv('CONFIG_PATH', '/config/config.json')) as json_file:
        json_data = json.load(json_file)
    for pvc_name in json_data:
        client.create_volume_from_backup(pvc_name)
    for pvc_name in json_data:
        client.finalize_restored_volume(pvc_name, json_data[pvc_name])


if __name__ == "__main__":
    print(f'longhorn-restore V{VERSION}')
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(stream=sys.stdout)
        ]
    )

    sleep_time = int(str(os.getenv('RESTORE_DELAY_IN_SECONDS', 1)))
    print('Use restore delay', sleep_time, 'sec')
    time.sleep(sleep_time)
    if bool(os.getenv('DISABLE_SSL_VERIFYCATION', False)):
        with no_ssl_verification():
            restor_backup()
    else:
        restor_backup()

