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

schema_constructors = StructType(
    [
        StructField("constructorId",IntegerType(), True)
        , StructField("constructorRef", StringType(), True)
        , StructField("name",StringType(), True)
        , StructField("nationality",StringType(), True)
        , StructField("url",StringType(), True)
    ]
)

# COMMAND ----------

df_constructors = spark.read \
                    .format("json") \
                    .schema(schema_constructors) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/constructors.json"
                    )

# COMMAND ----------

df_constructors = df_constructors \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .drop("url")

# COMMAND ----------

func_IngestOverwriteFile(df_constructors, "f1_processed", "constructors", varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.constructors
# MAGIC ZORDER BY (constructor_id)

# COMMAND ----------

df_constructors.unpersist()
del df_constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors
