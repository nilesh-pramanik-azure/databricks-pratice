# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Pit Stops (JSON) file in 4 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the JSON File**<br>
# MAGIC **Step-3: Renaming and Add Audit Columns**<br>
# MAGIC **Step-4: Writing the Pit Stops.json into Silver Container**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Prepare the accurate schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

schema_pit_stops = StructType(
    [
        StructField("raceId",IntegerType(), True)
        , StructField("driverId",IntegerType(), True)
        , StructField("stop",StringType(), True)
        , StructField("lap",IntegerType(), True)
        , StructField("time",StringType(), True)
        , StructField("duration",StringType(), True)
        , StructField("milliseconds",LongType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the JSON File

# COMMAND ----------

df_pit_stops = spark.read \
    .format("json") \
    .schema(schema_pit_stops) \
    .option("multiLine", True) \
    .load(
        path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/pit_stops.json"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming and Add Audit Columns

# COMMAND ----------

df_pit_stops = df_pit_stops \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Pit Stops.json into Silver Container

# COMMAND ----------

df_pit_stops.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(
        path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/pit_stops"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the parquet file**

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{varStorageAccntName}/{varSilverContainerName}/pit_stops"))
