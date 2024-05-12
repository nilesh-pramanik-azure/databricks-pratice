# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Mounting and Unmounting the Storage Container for Azure Storage

# COMMAND ----------

def func_MountUnmountContainer(varStorageAccountName, varContainerName, dictSecrets, varOperation):

    status = True

    if varOperation.upper() == "MOUNT":

        for varMountDetails in dbutils.fs.mounts():

            if varMountDetails.mountPoint == f"/mnt/{varStorageAccountName}/{varContainerName}":

                print(f"Already mount point created for : /mnt/{varStorageAccountName}/{varContainerName}")
                print(f"No need to re-mount... Exiting!")
                status = False
                break;
            
        
        if status:

            print(f"Mount Point Not Available: /mnt/{varStorageAccountName}/{varContainerName}")
            print("Will be mounting the above Mount Point")

            configs = {"fs.azure.account.auth.type": "OAuth",
                    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    "fs.azure.account.oauth2.client.id": dictSecrets["clientId"],
                    "fs.azure.account.oauth2.client.secret": dictSecrets["clientSecret"],
                    "fs.azure.account.oauth2.client.endpoint": f'https://login.microsoftonline.com/{dictSecrets["tenantId"]}/oauth2/token'}

            # Optionally, you can add <directory-name> to the source URI of your mount point.
            dbutils.fs.mount(
            source = f"abfss://{varContainerName}@{varStorageAccountName}.dfs.core.windows.net/",
            mount_point = f"/mnt/{varStorageAccountName}/{varContainerName}",
            extra_configs = configs)

            print(f"Mount Point: /mnt/{varStorageAccountName}/{varContainerName} created scuccessfully... Exiting!")


    elif varOperation.upper() == "UNMOUNT":

        for varMountDetails in dbutils.fs.mounts():

            if varMountDetails.mountPoint == f"/mnt/{varStorageAccountName}/{varContainerName}":
                
                print(f"Mountpoint Available: /mnt/{varStorageAccountName}/{varContainerName}")
                dbutils.fs.unmount(f"/mnt/{varStorageAccountName}/{varContainerName}")
                status = False
                break;

        if status:
            print(f"Mountpoint Not Available: /mnt/{varStorageAccountName}/{varContainerName}")
            print("Unmount not needed... Exiting!")           






