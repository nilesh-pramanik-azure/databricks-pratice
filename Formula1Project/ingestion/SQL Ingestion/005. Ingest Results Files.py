# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_raw.results
          (
              resultId INTEGER
              , raceId INTEGER
              , driverId INTEGER
              , constructorId INTEGER
              , number INTEGER
              , grid INTEGER
              , position INTEGER
              , positionText VARCHAR(100)
              , positionOrder INTEGER
              , points INTEGER
              , laps INTEGER
              , time VARCHAR(100)
              , milliseconds INTEGER
              , fastestLap INTEGER
              , rank INTEGER
              , fastestLapTime VARCHAR(100)
              , fastestLapSpeed DECIMAL(25,2)
              , statusId INTEGER

          )
          USING JSON
          LOCATION "/mnt/{varStorageAccntName}/{varBrozeContainerName}/results.json"
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From f1_raw.results
