-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Creating normal view whose life cycle is unless it is dropped.

-- COMMAND ----------

USE demo1

-- COMMAND ----------

show views in demo1

-- COMMAND ----------

CREATE OR REPLACE VIEW demo1.view_normal
AS
SELECT race_year, team, sum(points) as total_points FROM demo1.race_results_external_sql
group by race_year, team

-- COMMAND ----------

show views in demo1

-- COMMAND ----------

desc extended view_normal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Creating Temporary Views in SQL

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW view_temp
AS
SELECT race_year, team, sum(points) as total_points FROM demo1.race_results_external_sql
group by race_year, team

-- COMMAND ----------

SELECT * FROM view_temp

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Creating Global Temporary Views in SQL
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_global_temp
AS
SELECT race_year, team, sum(points) as total_points FROM demo1.race_results_external_sql
group by race_year, team

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.view_global_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("global_temp.view_global_temp").cache
-- MAGIC spark.table("global_temp.view_global_temp").count
-- MAGIC spark.catalog.uncacheTable("global_temp.view_global_temp")
