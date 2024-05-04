# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Constructors (JSON) file in 4 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the JSON File**<br>
# MAGIC **Step-3: Renaming, Dropping and Add Audit Columns**<br>
# MAGIC **Step-4: Writing the Constructors.json into Silver Container**

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

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

schema_constructor = StructType(
    [
        StructField("constructorId",IntegerType(), True)
        , StructField("constructorRef", StringType(), True)
        , StructField("name",StringType(), True)
        , StructField("nationality",StringType(), True)
        , StructField("url",StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the JSON File

# COMMAND ----------

df_constructors = spark.read \
    .format("json") \
    .schema(schema_constructor) \
    .load(path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming, Dropping and Add Audit Columns

# COMMAND ----------

df_constructors = df_constructors \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumn("source_system", lit(varSource)) \
    .withColumn("job_id", lit(varJobId)) \
    .select(
        col("constructorId").alias("contructor_id")
        , col("constructorRef").alias("constructor_ref")
        , col("name")
        , col("nationality")
        , col("ingestion_dt")
        , col("source_system")
        , col("job_id")
    )

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Constructors.json into Silver Container

# COMMAND ----------

df_constructors.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the parquet file**

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")
