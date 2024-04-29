# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by SAS Token in the Cluster using Secret Scope
# MAGIC * Get the SAS Token from secret scope<br>
# MAGIC * Set the spark configuration in the Cluster Spark Configuration like below with **KEY VALUE** Pair<br>
# MAGIC <img src="/files/tables/Cluster_Scope___SAS_Token_Key_Secret_Scope.jpg" width="400" height="200" />
# MAGIC * List the files available in the container "bronze"<br>
# MAGIC * Read the file "circuit.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Container Bronze**

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "abfss://bronze@datalakedbpractice.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying circuits.csv**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Sub-Folder**

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Display a file from Sub-Folder**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times/lap_times_split_1.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Try to access Silver Container through SAS Token created on Bronze Container**

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "abfss://silver@datalakedbpractice.dfs.core.windows.net"
