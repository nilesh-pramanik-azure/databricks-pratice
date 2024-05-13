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

df_races = spark.read \
                    .format("csv") \
                    .schema(schema_races) \
                    .option("header", True) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/races.csv"
                    )

# COMMAND ----------

df_races = df_races \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id") \
    .drop("url")

# COMMAND ----------

func_IngestOverwriteFile(df_races, "f1_processed", "races", varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.races
# MAGIC ZORDER BY (race_id)

# COMMAND ----------

df_races.unpersist()
del df_races

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races
