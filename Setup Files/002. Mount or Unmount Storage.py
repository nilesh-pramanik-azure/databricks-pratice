# Databricks notebook source
# MAGIC %md
# MAGIC **Creating an User Defined Function to Mount and Unmount the Storage Account as per User's request**

# COMMAND ----------

def funcMountUnmountStorage(dictMountDetails, varStorageAccountName, varContainerName, varUserRequest):

    # Creating the Mount Full Path

    varMountFullPath=f'/mnt/{varStorageAccountName}/{varContainerName}'
    varBool=True
    
    if varUserRequest.upper() == "MOUNT":

        for varMountInfo in dbutils.fs.mounts():

            if varMountFullPath in varMountInfo.mountPoint:

                print(f'Container {varContainerName} of Storage Account {varStorageAccountName} is already Mounted')
                print(f'Mount Path = {varMountFullPath}')
                print("Exiting the Function...")
                varBool=False
                break

        if varBool:

            print(f'Container {varContainerName} is not Mounted. Mount process will Start.')

            configs = {"fs.azure.account.auth.type": "OAuth",
                    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    "fs.azure.account.oauth2.client.id": dictMountDetails["clientID"],
                    "fs.azure.account.oauth2.client.secret": dictMountDetails["clientSecret"],
                    "fs.azure.account.oauth2.client.endpoint": f'https://login.microsoftonline.com/{dictMountDetails["tenantID"]}/oauth2/token'}

            dbutils.fs.mount(
            source = f'abfss://{varContainerName}@{varStorageAccountName}.dfs.core.windows.net/',
            mount_point = varMountFullPath,
            extra_configs = configs)

            print(f'Container {varContainerName} Mounted successfully.')
            print(f'Mount Path = {varMountFullPath}')
            print("Exiting the Function...")           

    elif varUserRequest.upper() == "UNMOUNT":

        for varMountInfo in dbutils.fs.mounts():

            if varMountFullPath in varMountInfo.mountPoint:

                print(f'Container {varContainerName} is Mounted. Unmount process will Start.')

                dbutils.fs.unmount(mount_point=varMountFullPath)
                
                print(f'Container {varContainerName} Unmounted successfully.')
                print("Exiting the Function...")
                varBool=False
                break               

        if varBool:

            print(f'Container {varContainerName} of Storage Account {varStorageAccountName} is NOT Mounted')
            print("Exiting the Function...")               


