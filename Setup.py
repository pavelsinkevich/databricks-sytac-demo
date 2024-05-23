# Databricks notebook source
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

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")
