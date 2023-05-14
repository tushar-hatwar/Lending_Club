# Databricks notebook source
# Modified Client ID
storage_account_name = "financestoragebig2023"
client_id            = "369c15e7-74ea-447d-8348-b088453c3ee5"
tenant_id            = "a9a1ad60-0851-40df-8e61-d9dd36cf0e8b"
client_secret        = "OI78Q~lRMsO-7ICQwe5sKY3CZka3YaXVsBkSWbMl"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# Command to unmount 
# dbutils.fs.unmount("/mnt/financestoragebig2023/processed-data")

# COMMAND ----------

# Command to unmount 
dbutils.fs.unmount("/mnt/financestoragebig2023/raw-data")
dbutils.fs.unmount("/mnt/financestoragebig2023/cleaned-data")
dbutils.fs.unmount("/mnt/financestoragebig2023/raw-data-temp")
dbutils.fs.unmount("/mnt/financestoragebig2023/processed-data")


# COMMAND ----------

# Command to mount raw-data
mount_adls("raw-data")

# Command to mount cleaned-data
mount_adls("cleaned-data")

# Command to mount raw-data-temp
mount_adls("raw-data-temp")

# Command to mount raw-data-temp
mount_adls("processed-data")

# COMMAND ----------

# Checking all the mounts
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/financestoragebig2023/"))

# COMMAND ----------

#listing files inside container
display(dbutils.fs.ls("/mnt/financestoragebig2023/cleaned-data"))
