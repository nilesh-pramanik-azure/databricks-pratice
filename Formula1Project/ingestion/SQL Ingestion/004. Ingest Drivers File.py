# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.drivers
    (
        driverId INTEGER
        , driverRef VARCHAR(100)
        , number INTEGER
        , code VARCHAR(100)
        , name STRUCT<
                        forename: VARCHAR(100)
                        , surname: VARCHAR(100)
                    >
        , surname VARCHAR(100)
        , dob DATE
        , nationality VARCHAR(100)
        , url VARCHAR(255)
    )
    USING JSON
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/drivers.json'

          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers
