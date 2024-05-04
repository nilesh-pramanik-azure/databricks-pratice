# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Races (CSV) file in 5 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the CSV File**<br>
# MAGIC **Step-3: Add the Calculated Date Column**<br>
# MAGIC **Step-4: Renaming & Dropping the Columns**<br>
# MAGIC **Step-5: Adding the Audit Column**<br>
# MAGIC **Step-6: Writing the Races.csv into Silver Container partitioning by Race_Year**

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

from pyspark.sql.functions import col, to_timestamp, current_timestamp, concat, lit

# COMMAND ----------

schema_races = StructType(
    [
        StructField("raceId", IntegerType(), True)
        , StructField("year", IntegerType(), True)
        , StructField("round", IntegerType(), True)
        , StructField("circuitId", IntegerType(), True)
        , StructField("name", StringType(), True)
        , StructField("date", StringType(), True)
        , StructField("time", StringType(), True)
        , StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the CSV File

# COMMAND ----------

df_races = spark.read \
    .format("csv") \
    .schema(schema_races) \
    .option("header", True) \
    .load(
        path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/races.csv"
    )

# COMMAND ----------

df_races.printSchema()

# COMMAND ----------

display(df_races.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Add the Calculated Date Column

# COMMAND ----------

df_races = df_races \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Renaming & Dropping the Columns

# COMMAND ----------

df_races = df_races \
    .select(
        col("raceID").alias("race_id")
        , col("year").alias("race_year")
        , col("round")
        , col("circuitId").alias("circuit_id")
        , col("name")
        , col("race_timestamp")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-5: Adding the Audit Column

# COMMAND ----------

df_races = df_races.withColumn("ingestion_dt", current_timestamp()) \
    .withColumn("source_system", lit(varSource)) \
    .withColumn("job_id", lit(varJobId))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-6: Writing the Races.csv into Silver Container

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_races.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("race_year") \
    .save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/races") 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the races files from Silver Container

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
