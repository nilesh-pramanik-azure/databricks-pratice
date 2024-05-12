# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

spark.sql(f"""
          CREATE DATABASE IF NOT EXISTS f1_processed
          COMMENT 'This database will hold the processed files as a part of increamental load'
          LOCATION '/mnt/{varStorageAccountName}/{varSilverContainerName}'
          """)

# COMMAND ----------

spark.sql(f"""
          CREATE DATABASE IF NOT EXISTS f1_presentation
          COMMENT 'This database will hold the presentation files as a part of increamental processed files'
          LOCATION '/mnt/{varStorageAccountName}/{varGoldContainerName}'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES
