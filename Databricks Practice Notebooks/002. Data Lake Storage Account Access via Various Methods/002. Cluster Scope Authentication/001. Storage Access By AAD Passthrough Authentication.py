# Databricks notebook source
# MAGIC %md
# MAGIC #####Storage Access by Azure Active Directory Passthrough Authentication
# MAGIC * In the storage, provide **"Storage Blob Data Contributor"** Role to the User
# MAGIC * Check the below box in the cluster<br>
# MAGIC <img src="/files/tables/AAD_Passthrough_Auth.jpg" width="400" height="200" />
# MAGIC * This way you can directly access the Storage

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@datalakedbpractice.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/circuits.csv"))
