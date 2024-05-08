-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####CREATING MANAGED TABLES USING PYTHON

-- COMMAND ----------

-- MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_raceresults = spark.read.format("parquet").load(
-- MAGIC     path = f"/mnt/{varStorageAccntName}/{varGoldContainerName}/race_results"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_raceresults.write.format("parquet").mode("overwrite").saveAsTable("demo.race_results_managed_python")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("""
-- MAGIC           select * from demo.race_results_managed_python
-- MAGIC           """))

-- COMMAND ----------

use demo

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####CREATING MANAGED TABLES USING SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_managed_sql
AS
SELECT race_year, race_name, race_date, circuit_location, driver_name, driver_number, driver_nationality, team, grid, points, created_date FROM demo.race_results_managed_python WHERE points >= 1

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESC TABLE EXTENDED demo.race_results_managed_sql

-- COMMAND ----------

DROP TABLE demo.race_results_managed_python
