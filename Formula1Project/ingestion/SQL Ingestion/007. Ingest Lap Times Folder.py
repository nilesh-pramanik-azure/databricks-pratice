# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.lap_times
    (
        raceId INTEGER
        , driverId INTEGER
        , lap INTEGER
        , position INTEGER
        , time varchar(100)
        , milliseconds decimal(25,2)
    )
    USING CSV
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/lap_times'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.lap_times
