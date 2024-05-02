# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Circuits (CSV) file in 5 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the CSV File**<br>
# MAGIC **Step-3: Renaming the Columns**<br>
# MAGIC **Step-4: Dropping a Column**<br>
# MAGIC **Step-5: Adding the Audit Column**<br>
# MAGIC **Step-6: Writing the Circuits.csv into Silver Container**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Prepare the accurate schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

schema_circuit = StructType(
    [
        StructField("circuitId", IntegerType(), True)
        , StructField("circuitRef", StringType(), True)
        , StructField("name", StringType(), True)
        , StructField("location", StringType(), True)
        , StructField("country", StringType(), True)
        , StructField("lat", DoubleType(), True)
        , StructField("lng", DoubleType(), True)
        , StructField("alt", IntegerType(), True)
        , StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the CSV File

# COMMAND ----------

df_circuit = spark.read.format("csv").schema(schema_circuit).option("header", True).load(path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/circuits.csv")

# COMMAND ----------

df_circuit.printSchema()

# COMMAND ----------

display(df_circuit.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming the Columns

# COMMAND ----------

df_circuit = df_circuit \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Dropping a Column

# COMMAND ----------

df_circuit = df_circuit.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-5: Adding the Audit Column

# COMMAND ----------

df_circuit = df_circuit \
    .withColumn("ingestion_dt", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-6: Writing the Circuits.csv into Silver Container

# COMMAND ----------

display(df_circuit)

# COMMAND ----------

df_circuit.write.format("parquet").mode("overwrite").save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading the file from Silver Container

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/circuits"))
