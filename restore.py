import longhorn
import longhorn_common
import os
import json

if __name__ == "__main__":
    with open(os.getenv('CONFIG_PATH', '/config/config.json')) as json_file:
        jsonData = json.load(json_file)
        longhornUrl = os.getenv('LONGHORN_URL')
        client = longhorn.Client(url=longhornUrl)
        wait_detached_vols = []
        volume_kstatus = {}
        for volumeHandle in jsonData:
            existingVolume = client.by_id_volume(id=volumeHandle)
            if existingVolume:
                print(f"Volume handle {volumeHandle} exists, skipping")
            else:
                print(f"Volume handle \"{volumeHandle}\" not found, restoring")
                bv = client.by_id_backupVolume(id=volumeHandle)

                if bv.lastBackupName:
                    lastBackup = bv.backupGet(name=bv.lastBackupName)
                else:
                    print("Backup for \"{volumeHandle}\" not found, skipping")
                    continue

                if "size" in jsonData[volumeHandle]:
                    volumeSize = jsonData[volumeHandle]["size"]
                else:
                    volumeSize = bv.size

                volume_kstatus[volumeHandle] = json.loads(lastBackup.labels.KubernetesStatus)

                client.create_volume(name=volumeHandle, size=volumeSize, fromBackup=lastBackup.url)
                wait_detached_vols.append(volumeHandle)

        for volumeHandle in jsonData:
            if volumeHandle in wait_detached_vols:
                kStatus = volume_kstatus[volumeHandle]
                createPV = jsonData[volumeHandle]["createPV"] if "createPV" in jsonData[volumeHandle] else True
                createPVC = jsonData[volumeHandle]["createPVC"] if "createPVC" in jsonData[volumeHandle] else True
                groups = jsonData[volumeHandle]["groups"] if "groups" in jsonData[volumeHandle] else []
                pvName = jsonData[volumeHandle]["pvName"] if "pvName" in jsonData[volumeHandle] else kStatus["pvName"]
                pvcName = jsonData[volumeHandle]["pvcName"] if "pvcName" in jsonData[volumeHandle] else kStatus["pvcName"]
                pvcNamespace = jsonData[volumeHandle]["pvcNamespace"] if "pvcNamespace" in jsonData[volumeHandle] else kStatus["namespace"]

                volume = longhorn_common.wait_for_volume_detached(client, volumeHandle)
                print(f"Restored volume {volumeHandle}")
                if groups:
                    for groupName in groups:
                        volume.recurringJobAdd(name=groupName, isGroup=True)

                if createPV:
                    longhorn_common.create_pv_for_volume(client, volume, pvName)
                    print(f"Restored PersistentVolume {pvName}")

                if createPVC:
                    longhorn_common.create_pvc_for_volume(client, pvcNamespace, volume, pvcName)
                    print(f"Restored PersistentVolumeClaim {pvcNamespace}")
