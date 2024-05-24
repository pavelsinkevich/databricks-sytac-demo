# Databricks notebook source
# unmount containers for demonstration
storageAccountName = "sytacdemo"
storageAccountAccessKey = dbutils.secrets.get(scope="key-vault-secret", key="storageAccountAccessKey")
medallionContainers = {"bronze", "silver", "gold", "checkpoint"}
for container in medallionContainers:
    mountPoint = f"/mnt/{container}/"
    if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.unmount(mountPoint)
            print("unmount succeeded!")
        except Exception as e:
            print("unmount exception", e)

# COMMAND ----------

# Outdated method: mount using wasbs
storageAccountName = "sytacdemo"
storageAccountAccessKey = dbutils.secrets.get(scope="key-vault-secret", key="storageAccountAccessKey")
medallionContainers = {"bronze", "silver", "gold", "checkpoint"}
for container in medallionContainers:
    mountPoint = f"/mnt/{container}/"
    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(container, storageAccountName),
            mount_point = mountPoint,
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
            print("mount succeeded!")
        except Exception as e:
            print("mount exception", e)

# COMMAND ----------

# Less outdated method: mount using abfss
storageAccountName = "sytacdemo"
client_id = dbutils.secrets.get(scope="key-vault-secret", key="ApplicationID")
tenant_id = dbutils.secrets.get(scope="key-vault-secret", key="tenantID")
client_secret = dbutils.secrets.get(scope="key-vault-secret", key="ApplicationSecret")

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": client_id,
"fs.azure.account.oauth2.client.secret": client_secret,
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

medallionContainers = {"bronze", "silver", "gold", "checkpoint"}
for container in medallionContainers:
    mountPoint = f"/mnt/{container}/"
    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        try:

            dbutils.fs.mount (
            source = "abfss://{}@{}.dfs.core.windows.net".format(container, storageAccountName),
            mount_point = mountPoint,
            extra_configs = configs
            )
            print("mount succeeded!")
        except Exception as e:
            print("mount exception", e)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_bronze
# MAGIC MANAGED LOCATION 'ext_loc_bronze';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_silver
# MAGIC MANAGED LOCATION 'ext_loc_silver';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_gold
# MAGIC MANAGED LOCATION 'ext_loc_gold'
