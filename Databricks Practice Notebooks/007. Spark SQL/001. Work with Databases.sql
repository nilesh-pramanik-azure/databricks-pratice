-- Databricks notebook source
-- MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####CREATING A TABLE IN CURRENT DATABASE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/circuits.csv")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CIRCUITS;
COPY INTO CIRCUITS
FROM '/mnt/datalakedbpractice/bronze/circuits.csv'
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM circuits

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC TABLE circuits

-- COMMAND ----------

DESCRIBE TABLE EXTENDED CIRCUITS

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo
COMMENT 'This is a DEMO Database'
WITH DBPROPERTIES (Prop1 = "ABCD", Prop2 ="XYZ");

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

USE DEMO

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESC DATABASE EXTENDED DEMO;
