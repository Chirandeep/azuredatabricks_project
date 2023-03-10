# Databricks notebook source
#dbutils.secrets.help()

# COMMAND ----------

#dbutils.secrets.listScopes()

# COMMAND ----------

#dbutils.secrets.list("formula1-scope")

# COMMAND ----------

#dbutils.secrets.get(scope="formula1-scope",key="formula1-clientid")

# COMMAND ----------

#for x in dbutils.secrets.get(scope="formula1-scope",key="formula1-clientid"):
   # print(x)

# COMMAND ----------

# Databricks notebook source
storage_account_name = "formula1pdl"
client_id            = dbutils.secrets.get(scope="formula1-scope",key="formula1-clientid")
tenant_id            = dbutils.secrets.get(scope="formula1-scope",key="formula1-tenentid")
client_secret        = dbutils.secrets.get(scope="formula1-scope",key="formula1-clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------


#container_name = "raw"
#dbutils.fs.mount(
 # source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  #mount_point = f"/mnt/{storage_account_name}/{container_name}",
  #extra_configs = configs)


# COMMAND ----------

#dbutils.fs.ls("/mnt/formula1dl/raw")

# COMMAND ----------

#dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1pdl/raw")


# COMMAND ----------

mount_adls("processed")

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("/mnt/formula1pdl/processed")


# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1pdl/presentation")

# COMMAND ----------


