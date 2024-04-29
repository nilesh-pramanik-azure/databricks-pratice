# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by Storage Access Key from Secret Scope
# MAGIC * Get the storage access key from secret scope<br>
# MAGIC * Set the spark configuration to setup the access<br>
# MAGIC * List the files available in the container "bronze"<br>
# MAGIC * Read the file "circuit.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC **Assigning the Access Key to a Local Variable**

# COMMAND ----------

storage_access_key = dbutils.secrets.get("databricks-practice-certification","storageAccessKey")

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting up Spark Configuration**

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakedbpractice.dfs.core.windows.net", 
    storage_access_key
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Container Bronze**

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@datalakedbpractice.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying circuits.csv**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Sub-Folder**

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Display a file from Sub-Folder**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times/lap_times_split_1.csv"))
