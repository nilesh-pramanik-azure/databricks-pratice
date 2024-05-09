# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS f1_raw.circuits
    (
        circuitId INTEGER
        , circuitRef VARCHAR(100)
        , name VARCHAR(100)
        , location VARCHAR(100)
        , country VARCHAR(100)
        , lat DECIMAL(25,2)
        , lng DECIMAL(25,2)
        , alt INTEGER
        , url VARCHAR(255)
    )
    USING CSV
    OPTIONS ('header'='true')
    LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/circuits.csv'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.circuits
