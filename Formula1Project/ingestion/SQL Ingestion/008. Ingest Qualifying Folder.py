# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.qualifying
    (
        qualifyId INTEGER
        , raceId INTEGER
        , driverId INTEGER
        , constructorId INTEGER
        , number INTEGER
        , position INTEGER
        , q1 STRING
        , q2 STRING
        , q3 STRING
    )
    USING JSON
    OPTIONS ('multiline' = 'true')
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/qualifying/*.json'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying
