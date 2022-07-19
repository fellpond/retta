# Databricks notebook source
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

        
def mount_adls(adlsAccountName, adlsContainerName, mountPoint, configs):
    dbutils.fs.mount(
        source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/",
        mount_point =mountPoint ,
        extra_configs = configs)

    
def setConfigAdls():
    # Application (Client) ID
    applicationId = dbutils.secrets.get(scope="adls",key="CLIENT-ID")    
    authenticationKey = dbutils.secrets.get(scope="adls",key="ClientSecret")    
    tenandId = dbutils.secrets.get(scope="adls",key="TENENT-ID")
    
    endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": applicationId,
               "fs.azure.account.oauth2.client.secret": authenticationKey,
               "fs.azure.account.oauth2.client.endpoint": endpoint}
    return configs

 

# COMMAND ----------

adlsAccountName = "rettadatalake"
adlsContainerName = ["energydlyin", "energydlydtg"]
mountPoints = ["/mnt/energy_dly_in", "/mnt/energy_dly_stg"]

container_mnt_map = dict(zip(adlsContainerName, mountPoint))


 
#unmount existing storage account
for mount in mountPoints:
    sub_unmount(mount)


#set configuration and mount adls containers to /mnt location 
configs=setConfigAdls()


for container, mount in container_mnt_map.items():
    mount_adls(adlsAccountName, container, mount, configs)
    


# COMMAND ----------


