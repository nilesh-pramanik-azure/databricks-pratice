# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

from pyspark.sql.functions import lit, concat

# COMMAND ----------

dbutils.widgets.text("source_system","Test Data")
dbutils.widgets.text("job_run_id","-9999")
dbutils.widgets.text("cycle_date","1800-01-01")

# COMMAND ----------

varSourceSystem=dbutils.widgets.get("source_system")
varJobRunId=dbutils.widgets.get("job_run_id")
varCycleDate=dbutils.widgets.get("cycle_date")

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

df_drivers = spark.read \
                    .format("json") \
                    .schema(schema_drivers) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/drivers.json"
                    )

# COMMAND ----------

df_drivers = df_drivers \
    .withColumn("fullname", concat(df_drivers.name.forename, lit(" "), df_drivers.name.surname)) \
    .drop("name") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumnRenamed("dob","birth_date") \
    .withColumnRenamed("fullname","name") \
    .drop("url")

# COMMAND ----------

func_IngestOverwriteFile(df_drivers, "f1_processed", "drivers", varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.drivers
# MAGIC ZORDER BY (driver_id)

# COMMAND ----------

df_drivers.unpersist()
del df_drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers
