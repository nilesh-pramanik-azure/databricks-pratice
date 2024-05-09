# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS f1_raw
COMMENT 'This database will hold all the tables from raw files'
LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/f1_raw_database'
""")
