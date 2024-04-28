# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by Storage Access Key in the Cluster
# MAGIC * Get the storage access key from storage account<br>
# MAGIC * Set the spark configuration in the Cluster Spark Configuration like below with **KEY VALUE** Pair<br>
# MAGIC <img src="/files/tables/Cluster_Scope___Storage_Access_Key.jpg" width="400" height="200" />
# MAGIC * List the files available in the container "bronze"<br>
# MAGIC * Read the file "circuit.csv"

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
