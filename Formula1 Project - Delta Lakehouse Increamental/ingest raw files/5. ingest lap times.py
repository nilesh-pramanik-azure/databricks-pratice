# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

from pyspark.sql.functions import lit, concat, current_timestamp, to_date

# COMMAND ----------

dbutils.widgets.text("source_system","Test Data")
dbutils.widgets.text("job_run_id","-9999")
dbutils.widgets.text("cycle_date","1800-01-01")

# COMMAND ----------

varSourceSystem=dbutils.widgets.get("source_system")
varJobRunId=dbutils.widgets.get("job_run_id")
varCycleDate=dbutils.widgets.get("cycle_date")

# COMMAND ----------

schema_lap_times = StructType(
    [
        StructField("raceId", IntegerType(), True)
        , StructField("driverId", IntegerType(), True)
        , StructField("lap", IntegerType(), True)
        , StructField("position", IntegerType(), True)
        , StructField("time", StringType(), True)
        , StructField("milliseconds", StringType(), True)
    ]
)

# COMMAND ----------

df_lap_times = spark.read \
                    .format("csv") \
                    .option("header",False) \
                    .schema(schema_lap_times) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/lap_times"
                    )

# COMMAND ----------

df_lap_times = df_lap_times \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("raceId","race_id")

# COMMAND ----------

listPartitionFields=["race_id"]
df_lap_times = func_AddAuditFields(df_lap_times, varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

if spark.catalog.tableExists("f1_processed.lap_times"):
    
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","True")

    dt_table = DeltaTable.forName(spark, "f1_processed.lap_times")

    df_temp = dt_table.toDF().filter("row_end >= current_timestamp()").alias("tgt") \
                            .join(
                                df_lap_times.alias("src"),
                                (col("tgt.race_id") == col("src.race_id"))
                                & (col("tgt.driver_id") == col("src.driver_id"))
                                & (col("tgt.lap") == col("src.lap"))
                                & (
                                    (col("tgt.position") != col("src.position"))
                                    | (col("tgt.time") != col("src.time"))
                                    | (col("tgt.milliseconds") != col("src.milliseconds"))
                                ),
                                "inner"
                            ) \
                            .selectExpr(
                                "Null as SurrRaceId",
                                "Null as SurrDriverId",
                                "Null as SurrLap",
                                "src.*"
                            )
    
    df_temp = df_temp.union(
                    df_lap_times.selectExpr(
                        "race_id as SurrRaceId",
                        "driver_id as SurrDriverId",
                        "lap as SurrLap",
                        "*"
                    )
                )
    
    dt_table.alias("tgt").merge(
        df_temp.alias("src"),
        "tgt.race_id = src.SurrRaceId \
        and tgt.driver_id = src.SurrDriverId \
        and tgt.lap = src.SurrLap \
        and tgt.row_end >= current_timestamp()" 
    ) \
    .whenMatchedUpdate(
        condition=(
            "tgt.position <> src.position \
            or tgt.time <> src.time \
            or tgt.milliseconds <> src.milliseconds"
        ),
        set={
            "row_end":current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_lap_times.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy(listPartitionFields) \
    .saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.lap_times
# MAGIC ZORDER BY (driver_id, lap)

# COMMAND ----------

df_lap_times.unpersist()
del df_lap_times

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times
