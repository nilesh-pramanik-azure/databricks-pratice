-- Databricks notebook source
-- MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

SHOW DATABASES
