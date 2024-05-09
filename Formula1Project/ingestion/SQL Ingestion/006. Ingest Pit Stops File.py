# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
    (
        raceId INTEGER
        , driverId INTEGER
        , stop VARCHAR(100)
        , lap INTEGER
        , time VARCHAR(100)
        , duration VARCHAR(100)
        , milliseconds LONG
    )
    USING JSON
    OPTIONS ('MULTILINE' = 'TRUE')
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/pit_stops.json'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops
