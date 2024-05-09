# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.races
    (
        raceId INTEGER
        , year INTEGER
        , round INTEGER
        , circuitId INTEGER
        , name VARCHAR(100)
        , date VARCHAR(100)
        , time VARCHAR(100)
        , url VARCHAR(255)
    )
    USING CSV
    OPTIONS ('HEADER' = 'TRUE')
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/races.csv'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.races
