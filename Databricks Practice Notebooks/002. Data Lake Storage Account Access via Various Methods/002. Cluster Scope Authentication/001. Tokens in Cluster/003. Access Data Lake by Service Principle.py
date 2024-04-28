# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by Service Principle in the Cluster
# MAGIC * Get the Service Principle Tokens<br>
# MAGIC * Set the spark configuration in the Cluster Spark Configuration like below with **KEY VALUE** Pair<br>
# MAGIC <img src="/files/tables/Cluster_Scope___Service_Principle.jpg" width="400" height="200" />
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

# MAGIC %fs
# MAGIC ls "abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times"

# COMMAND ----------

# MAGIC %md
# MAGIC **Display a file from Sub-Folder**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times/lap_times_split_1.csv"))
