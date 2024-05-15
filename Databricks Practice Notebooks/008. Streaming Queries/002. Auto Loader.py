# Databricks notebook source


# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakedbpractice/bronze/2021-03-21"))

# COMMAND ----------

num=1
dbutils.fs.cp("/mnt/datalakedbpractice/bronze/2021-03-21/drivers.json",f"/mnt/datalakedbpractice/bronze/2024-03-21/{num}drivers.json")

# COMMAND ----------

spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format","json") \
        .option("cloudFiles.schemaLocation","dbfs:/FileStore/tables/schema") \
        .load("/mnt/datalakedbpractice/bronze/2024-03-21") \
        .writeStream \
        .option("checkpointLocation","dbfs:/FileStore/tables/schema") \
        .table("autostreamdriver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from autostreamdriver

# COMMAND ----------

for num in range(50,100001):
    dbutils.fs.cp("/mnt/datalakedbpractice/bronze/2021-03-21/drivers.json",f"/mnt/datalakedbpractice/bronze/2024-03-21/{num}drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history autostreamdriver
