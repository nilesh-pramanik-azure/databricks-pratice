# Databricks notebook source
# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

for dbdetails in spark.catalog.listDatabases():
    dbname=dbdetails.name
    if dbname != "default":
        spark.sql(f"""DROP DATABASE IF EXISTS {dbname} CASCADE """)
        print(f"Database: '{dbname}' dropped successfully")
