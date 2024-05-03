# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Results (JSON) file in 4 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the JSON File**<br>
# MAGIC **Step-3: Renaming, Dropping and Add Audit Columns**<br>
# MAGIC **Step-4: Writing the Results.json into Silver Container partition by Race ID**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Prepare the accurate schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

# COMMAND ----------

from pyspark.sql.functions import lit, col, current_timestamp

# COMMAND ----------

schema_results = StructType(
    [
        StructField("resultId",IntegerType(),True)
        , StructField("raceId",IntegerType(),True)
        , StructField("driverId",IntegerType(),True)
        , StructField("constructorId",IntegerType(),True)
        , StructField("number",IntegerType(),True)
        , StructField("grid",IntegerType(),True)
        , StructField("position",IntegerType(),True)
        , StructField("positionText",StringType(),True)
        , StructField("positionOrder",IntegerType(),True)
        , StructField("points",IntegerType(),True)
        , StructField("laps",IntegerType(),True)
        , StructField("time",StringType(),True)
        , StructField("milliseconds",LongType(),True)
        , StructField("fastestLap",IntegerType(),True)
        , StructField("rank",IntegerType(),True)
        , StructField("fastestLapTime",StringType(),True)
        , StructField("fastestLapSpeed",DoubleType(),True)
        , StructField("statusId",IntegerType(),True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the JSON File

# COMMAND ----------

df_results = spark.read \
                .format("json") \
                .schema(schema_results) \
                .load(path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming, Dropping and Add Audit Columns

# COMMAND ----------

df_results = df_results \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Results.json into Silver Container partition by Race ID

# COMMAND ----------

df_results.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("race_id") \
    .save(
        path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/results"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying the Parquet File**

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{varStorageAccntName}/{varSilverContainerName}/results"))
