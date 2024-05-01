# Databricks notebook source
# MAGIC %md
# MAGIC #####Before enabling DBFS Broswer, lets see what is there in the DBFS root folder.
# MAGIC **DBFS File Broswer Disabled:**<br><img src="/files/tables/DBFS_Disabled.jpg" width="400" height="200" />

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Points to Note
# MAGIC - [x] In the above result - File Store was not supposed to be diplayed, but it got displayed as we have already stored some files beforehand in that folder.
# MAGIC - [x] FileStore should have been displayed after a single file was added in the DBFS File Browser

# COMMAND ----------

# MAGIC %md
# MAGIC #####Before enabling DBFS Broswer, lets see what is there in the DBFS root folder.
# MAGIC **DBFS File Broswer Disabled:**<br><img src="/files/tables/DBFS_Enabled.jpg" width="400" height="200" />

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Dive deep into the filestore folder**

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))
