-- Databricks notebook source
CREATE TABLE test_stream_data
(
  id INTEGER,
  name STRING
)

-- COMMAND ----------

describe table extended test_stream_data

-- COMMAND ----------

CREATE TABLE output_stream_data
(
  id INTEGER,
  name STRING
  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.table("test_stream_data").createOrReplaceTempView("view_stream_data")
-- MAGIC

-- COMMAND ----------

insert into test_stream_data values (4,'Arnab');
insert into test_stream_data values (5,'Shankha');
insert into test_stream_data values (6,'Riku');

-- COMMAND ----------

select * from view_stream_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("view_stream_data") \
-- MAGIC     .writeStream \
-- MAGIC     .trigger(processingTime="20 seconds") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .option("checkpointLocation","dbfs:/FileStore/tables") \
-- MAGIC     .table("output_stream_data")    

-- COMMAND ----------

select * from default.output_stream_data order by 1
