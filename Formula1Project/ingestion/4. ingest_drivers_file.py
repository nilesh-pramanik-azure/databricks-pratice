# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingesting Drivers (JSON) file in 4 steps
# MAGIC **Step-1: Prepare the accurate schema**<br>
# MAGIC **Step-2: Reading the JSON File**<br>
# MAGIC **Step-3: Renaming, Dropping and Add Audit Columns**<br>
# MAGIC **Step-4: Writing the Drivers.json into Silver Container**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Prepare the accurate schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DateType

# COMMAND ----------

from pyspark.sql.functions import lit, col, current_timestamp, concat

# COMMAND ----------

schema_drivers = StructType(
    [
        StructField("driverId", IntegerType(), True)
        , StructField("driverRef", StringType(), True)
        , StructField("number", IntegerType(), True)
        , StructField("code", StringType(), True)
        , StructField("name",
            StructType(
                [
                    StructField("forename", StringType(), True)
                    , StructField("surname", StringType(), True)
                ]
            )
        )
        , StructField("dob", DateType(), True)
        , StructField("nationality", StringType(), True)
        , StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2: Reading the JSON File

# COMMAND ----------

df_drivers = spark.read \
    .format("json") \
    .schema(schema_drivers) \
    .load(path=f"/mnt/{varStorageAccntName}/{varBrozeContainerName}/drivers.json")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3: Renaming, Dropping and Add Audit Columns

# COMMAND ----------

df_drivers = df_drivers \
    .withColumn("ingestion_dt", current_timestamp()) \
    .withColumn("fullname", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .drop("name") \
    .drop("url") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumnRenamed("fullname", "name") \
    .withColumnRenamed("dob", "birth_dt")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4: Writing the Drivers.json into Silver Container

# COMMAND ----------

df_drivers.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the parquet file**

# COMMAND ----------

display(spark.read.format("parquet").load(path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/drivers"))
