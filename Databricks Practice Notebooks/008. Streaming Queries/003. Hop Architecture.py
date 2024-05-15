# Databricks notebook source
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format","json") \
    .option("cloudFiles.schemaLocation","dbfs:/FileStore/tables/Customer") \
    .load("/mnt/datalakedbpractice/bronze/Customer") \
    .createOrReplaceTempView("raw_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RAW_FILE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_raw_temp
# MAGIC as (
# MAGIC SELECT custid, bookcount, dob, name, nationality, input_file_name() as filename, current_timestamp() as ingestion_time
# MAGIC from RAW_FILE)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view_raw_temp

# COMMAND ----------

spark.table("view_raw_temp") \
    .writeStream \
    .format("delta") \
    .trigger(processingTime="2 seconds") \
    .outputMode("append") \
    .option("checkpointLocation","dbfs:/FileStore/tables/Customer") \
    .table("raw_table")

# COMMAND ----------

spark.readStream \
    .table("raw_table") \
    .createOrReplaceTempView("view_bronze")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_bronze_temp
# MAGIC AS
# MAGIC (
# MAGIC   SELECT custid as customer_id, concat(name:forename,' ',name:surname) as fullname, nationality, to_date(dob,'yyyy-MM-dd') as birth_date, bookcount as book_count
# MAGIC   from view_bronze
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view_bronze_temp

# COMMAND ----------

spark.table("view_bronze_temp") \
    .writeStream \
    .format("delta") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .option("checkpointLocation","dbfs:/FileStore/tables/Customer/Silver") \
    .table("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table

# COMMAND ----------

spark.readStream \
    .table("silver_table") \
    .createOrReplaceTempView("view_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_gold_temp
# MAGIC AS
# MAGIC (
# MAGIC   SELECT CUSTOMER_ID, FULLNAME, NATIONALITY, BIRTH_DATE, SUM(BOOK_COUNT) AS TOT_BOOK_COUNT
# MAGIC   FROM view_gold 
# MAGIC   GROUP BY CUSTOMER_ID, FULLNAME, NATIONALITY, BIRTH_DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM VIEW_GOLD_TEMP

# COMMAND ----------

spark.table("view_gold_temp") \
    .writeStream \
    .format("delta") \
    .trigger(availableNow=True) \
    .outputMode("complete") \
    .option("checkpointLocation","dbfs:/FileStore/tables/Customer/Gold") \
    .table("gold_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_table

# COMMAND ----------

for i in spark.streams.active:
    print(f"Streaming: {i.name} for ID = {i.id}")
    i.stop()
    i.awaitTermination()
