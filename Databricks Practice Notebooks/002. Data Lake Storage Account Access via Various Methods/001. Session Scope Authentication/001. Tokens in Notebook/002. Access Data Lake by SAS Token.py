# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by SAS Token
# MAGIC * Generate the SAS Token for Container Bronze<br>
# MAGIC * Provide the respective permission to the user for the container
# MAGIC * Set the spark configuration to setup the access<br>
# MAGIC * Try to read the files from container "silver" <br>
# MAGIC * List the files available in the container "bronze"<br>
# MAGIC * Read the file "circuit.csv"
# MAGIC * Read one more file from the sub-folder

# COMMAND ----------

# MAGIC %md
# MAGIC **Assigning the SAS Token to a Local Variable**

# COMMAND ----------

SASToken="sp=rl&st=2024-04-28T10:45:45Z&se=2024-12-30T18:45:45Z&spr=https&sv=2022-11-02&sr=c&sig=yp3h1a43Gs3VkjtKhWuByA2s6KLWNTmQnm%2Bep2WRkHU%3D"

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting up Spark Configuration**

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakedbpractice.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalakedbpractice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalakedbpractice.dfs.core.windows.net", SASToken)

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