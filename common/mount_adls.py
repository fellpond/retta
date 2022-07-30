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
adlsContainerNames = ['bronze', 'silver', 'gold']



#set configuration and mount adls containers to /mnt location 
configs=setConfigAdls()
for container in adlsContainerNames:
    if any(mount.mountPoint == '/mnt/'+ container for mount in dbutils.fs.mounts()):
        pass
    else:
        mount_adls(adlsAccountName, container, '/mnt/'+ container, configs)


