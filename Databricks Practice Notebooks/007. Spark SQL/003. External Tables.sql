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
-- MAGIC df_raceresults.write.format("parquet").mode("overwrite").option("path","dbfs:/user/hive/warehouse/demo.db/race_results_external_python").saveAsTable("demo.race_results_external_python")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("""
-- MAGIC           select * from demo.race_results_external_python
-- MAGIC           """))

-- COMMAND ----------

use demo

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####CREATING MANAGED TABLES USING SQL

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_external_sql
LOCATION 'dbfs:/user/hive/warehouse/demo.db/race_results_external_sql'
COMMENT 'SAMPLE EXTERNAL TABLE OF RACE RESULTS'
AS
SELECT race_year, race_name, race_date, circuit_location, driver_name, driver_number, driver_nationality, team, grid, points, created_date FROM demo.race_results_external_python WHERE points >= 1

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESC TABLE EXTENDED demo.race_results_external_sql

-- COMMAND ----------

DROP TABLE demo.race_results_external_sql

-- COMMAND ----------

SELECT * FROM demo.race_results_external_sql;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_external_sql
LOCATION 'dbfs:/user/hive/warehouse/demo.db/race_results_external_sql'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####I AM NOT RELOADING THE DATA... BUT THE DATA WILL BE AVAILABLE IN THE TABLE

-- COMMAND ----------

select * from demo.race_results_external_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####A REAL WORLD EXAMPLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC           create database if not exists demo1
-- MAGIC             location "/mnt/{varStorageAccntName}/{varBrozeContainerName}/temporarydatabase"
-- MAGIC             """)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo1.race_results_external_sql
USING PARQUET
LOCATION 'dbfs:/user/hive/warehouse/demo1.db/race_results_external_sql'
COMMENT 'SAMPLE EXTERNAL TABLE OF RACE RESULTS'
AS
SELECT race_year, race_name, race_date, circuit_location, driver_name, driver_number, driver_nationality, team, grid, points, created_date FROM demo.race_results_external_python WHERE points >= 1

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DROP TABLE demo1.race_results_external_sql;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo1.race_results_external_sql
USING PARQUET
LOCATION 'dbfs:/user/hive/warehouse/demo1.db/race_results_external_sql'
COMMENT 'SAMPLE EXTERNAL TABLE OF RACE RESULTS'

-- COMMAND ----------

SELECT * FROM demo1.race_results_external_sql

-- COMMAND ----------

DESC TABLE EXTENDED demo1.race_results_external_sql
