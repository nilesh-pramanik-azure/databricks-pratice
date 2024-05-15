-- Databricks notebook source
-- MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC varBronzeContainerName

-- COMMAND ----------

SELECT * FROM text.`/mnt/datalakedbpractice/bronze/2021-03-28/drivers.json`

-- COMMAND ----------

SELECT * from binaryFile.`dbfs:/FileStore/tables/Cluster_Scope___SAS_Token_Key_Secret_Scope.jpg`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW view_circuits
AS
SELECT * FROM csv.`/mnt/datalakedbpractice/bronze/2021-03-28/circuits.csv`

-- COMMAND ----------

SELECT input_file_name(), input_file_block_start(), input_file_block_length(), * FROM csv.`/mnt/datalakedbpractice/bronze/2021-03-28/circuits.csv`

-- COMMAND ----------

select * from view_circuits

-- COMMAND ----------

create or replace table table_circuits
as
select * From csv.`/mnt/datalakedbpractice/bronze/2021-03-28/circuits.csv`

-- COMMAND ----------

desc table_circuits

-- COMMAND ----------

select * From table_circuits version as of 0

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false


-- COMMAND ----------

vacuum table_circuits retain 0 hours

-- COMMAND ----------

desc history table_circuits

-- COMMAND ----------

select * from table_circuits@v3

-- COMMAND ----------

show tables

-- COMMAND ----------

CREATE OR REPLACE TABLE table_drivers
AS
SELECT * fROM JSON.`/mnt/datalakedbpractice/bronze/2021-03-28/drivers.json`

-- COMMAND ----------

select * from table_drivers;

-- COMMAND ----------

desc table_drivers

-- COMMAND ----------

select name:forename, name:surname from table_drivers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_temp = spark.createDataFrame(
-- MAGIC     [
-- MAGIC         ("E01", [{"Name":"Arnab Desarkar", "Details":{"Age":36,"Gender":"M"}},{"Name":"Shankha Desarkar", "Details":{"Age":36,"Gender":"M"}}]),
-- MAGIC         ("E01", [{"Name":"Aindrila Bose", "Details":{"Age":27,"Gender":"F"}}])
-- MAGIC     ], schema=["EmpId","Details"]
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_temp)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_temp.createOrReplaceTempView("view_employee2")

-- COMMAND ----------

select * from view_employee2

-- COMMAND ----------


select empid, details, filter(details, i -> i.Details = '{Age=27, Gender=F}') as x from view_employee2


-- COMMAND ----------

select empid, from_json(explode(details), schema_of_json('[{"Details": "{Age=36, Gender=M}", "Name": "Arnab Desarkar"}]') ) from view_employee

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

select * from races

-- COMMAND ----------

select race_year, collect_set(date) from races
group by race_year

-- COMMAND ----------

use default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_temp = spark.createDataFrame(
-- MAGIC     [
-- MAGIC         (["E01", "Arnab Desarkar", [36,39], "M", 70],["E01", "Shankha Desarkar", [36,39], "M",90]),
-- MAGIC         (["E01", "Arnab Desarkar", [40,39], "M", 65]),
-- MAGIC         (["E01", "Arnab Desarkar", [40,39], "M", 50]),
-- MAGIC     ], schema=["EmpId","Name", "Mylist","Gender","Amount"]
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_temp.createOrReplaceTempView("view_employee")

-- COMMAND ----------

SELECT * FROM view_employee

-- COMMAND ----------

select empid, collect_set(name), collect_list(age), array_distinct(flatten(collect_list(age)))
from view_employee
group by empid

-- COMMAND ----------

select * from text.`dbfs:/FileStore/tables/a.json`

-- COMMAND ----------

CREATE FUNCTION default.MYFACTORIAL(n integer)
returns integer
RETURN CASE
        WHEN N <= 0 THEN 1
        WHEN N = 1 THEN 1
        ELSE N * default.MYFACTORIAL1(N-1)
        END

-- COMMAND ----------

SELECT MYFACTORIAL(5)
