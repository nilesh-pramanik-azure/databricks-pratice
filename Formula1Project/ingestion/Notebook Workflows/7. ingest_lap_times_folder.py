# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting ALL Lap Times (CSV) file in from the folder in 5 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the CSV File**<br>
# MAGIC **Step-3: Renaming & Add the Audit Columns**<br>
# MAGIC **Step-4: Writing the Lap Times.csv into Silver Container**

# COMMAND ----------

dbutils.widgets.text("source","Default","Source System:")
dbutils.widgets.text("jobid","0","Job ID:")
varSource=dbutils.widgets.get("source")
varJobId=int(dbutils.widgets.get("jobid"))

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Prepare the accurate schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

schema_lap_times = StructType(
    [
        StructField("raceId", IntegerType(), True)
        , StructField("driverId", IntegerType(), True)
        , StructField("lap", IntegerType(), True)
        , StructField("position", IntegerType(), True)
        , StructField("time", StringType(), True)
        , StructField("milliseconds", DoubleType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the CSV File

# COMMAND ----------

df_lap_times = spark.read \
    .format("csv") \
    .schema(schema_lap_times) \
    .option("header", False) \
    .load(
        path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/lap_times"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming & Add the Audit Columns

# COMMAND ----------

df_lap_times = df_lap_times \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumn("source_system", lit(varSource)) \
    .withColumn("job_id", lit(varJobId))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Lap Times.csv into Silver Container

# COMMAND ----------

df_lap_times.write.format("parquet").mode("overwrite").save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading the file from Silver Container

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
