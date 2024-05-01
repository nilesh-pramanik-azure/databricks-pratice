# Databricks notebook source
# MAGIC %md
# MAGIC #####To mount a DBFS, we need to create a Service Principle
# MAGIC * Service Principle is already created - "serviceprinciple-databricks-practice-certification"
# MAGIC * We will be using the existing Service Principle Credentials to mount the Storage Account
# MAGIC * TenantID key has been renamed to "newTenantID"
# MAGIC * Root Mount Path will be "/mnt"
# MAGIC * Our sample Mount will be "/mnt/storageAccountName/ContainerName"

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Default mounts available in the workspace

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Mounting the Storage Account

# COMMAND ----------

dbutils.secrets.list("databricks-practice-certification")

# COMMAND ----------

tenantID=dbutils.secrets.get("databricks-practice-certification","newTenantID")

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get("databricks-practice-certification","clientID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("databricks-practice-certification","clientSecret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalakedbpractice.dfs.core.windows.net/",
  mount_point = "/mnt/datalakedbpractice/bronze",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Let's check the DBFS Root Folder and dive deep into them
# MAGIC #####We should get a new folde named "mnt"

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakedbpractice"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakedbpractice/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Let's read the circuits.csv file

# COMMAND ----------

display(spark.read.load(path="dbfs:/mnt/datalakedbpractice/bronze/circuits.csv", format="csv"))

# COMMAND ----------

display(spark.read.csv(path="/mnt/datalakedbpractice/bronze/circuits.csv"))
