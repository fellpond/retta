# Databricks notebook source
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

# COMMAND ----------

adlsAccountName = "rettadatalake"
adlsContainerName = "energydlyin"
mountPoint = "/mnt/energy_dly_in"
 
# Application (Client) ID
applicationId = dbutils.secrets.get(scope="adls",key="CLIENT-ID")
 
 
# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="adls",key="ClientSecret")

 
# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="adls",key="TENENT-ID")
 
 
endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"



#unmount adls before start 

sub_unmount(mountPoint)


# COMMAND ----------




 

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}


# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/",
  mount_point =mountPoint ,
  extra_configs = configs)
