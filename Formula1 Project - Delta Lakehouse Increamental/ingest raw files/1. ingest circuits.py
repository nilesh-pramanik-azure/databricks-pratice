# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.text("source_system","Test Data")
dbutils.widgets.text("job_run_id","-9999")
dbutils.widgets.text("cycle_date","1800-01-01")

# COMMAND ----------

varSourceSystem=dbutils.widgets.get("source_system")
varJobRunId=dbutils.widgets.get("job_run_id")
varCycleDate=dbutils.widgets.get("cycle_date")

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

df_circuits = spark.read \
                    .format("csv") \
                    .schema(schema_circuit) \
                    .option("header",True) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/circuits.csv"
                    )

# COMMAND ----------

df_circuits = df_circuits \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .drop("url")

# COMMAND ----------

func_IngestOverwriteFile(df_circuits, "f1_processed", "circuits", varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.circuits
# MAGIC ZORDER BY (circuit_id)

# COMMAND ----------

df_circuits.unpersist()
del df_circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits
