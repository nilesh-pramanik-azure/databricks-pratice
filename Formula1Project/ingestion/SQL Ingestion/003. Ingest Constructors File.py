# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_raw.constructors
    (
        constructorId INTEGER
        , constructorRef VARCHAR(100)
        , name VARCHAR(100)
        , nationality VARCHAR(100)
        , url VARCHAR(255)
    )
    USING JSON
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/constructors.json'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.constructors
