# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting ALL Qualifying (JSON) files in from the folder in 4 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the JSON File**<br>
# MAGIC **Step-3: Renaming & Add the Audit Columns**<br>
# MAGIC **Step-4: Writing the Qualifying.json into Silver Container**

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

schema_qualifying = StructType(
    [
        StructField("qualifyId", IntegerType(), True)
        , StructField("raceId", IntegerType(), True)
        , StructField("driverId", IntegerType(), True)
        , StructField("constructorId", IntegerType(), True)
        , StructField("number", IntegerType(), True)
        , StructField("position", IntegerType(), True)
        , StructField("q1", StringType(), True)
        , StructField("q2", StringType(), True)
        , StructField("q3", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the JSON Files

# COMMAND ----------

df_qualifying = spark.read \
    .format("json") \
    .schema(schema_qualifying) \
    .option("multiLine", True) \
    .load(
        path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/qualifying/*.json"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming & Add the Audit Columns

# COMMAND ----------

df_qualifying = df_qualifying \
    .withColumnRenamed("qualifyingId","qualifying_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumn("source_system", lit(varSource)) \
    .withColumn("job_id", lit(varJobId))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Qualifying.json into Silver Container

# COMMAND ----------

df_qualifying.write.format("parquet").mode("overwrite").save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading the file from Silver Container

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
