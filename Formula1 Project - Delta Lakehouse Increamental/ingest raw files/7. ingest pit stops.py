# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType, LongType

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

schema_pit_stops = StructType(
    [
        StructField("raceId",IntegerType(), True)
        , StructField("driverId",IntegerType(), True)
        , StructField("stop",StringType(), True)
        , StructField("lap",IntegerType(), True)
        , StructField("time",StringType(), True)
        , StructField("duration",StringType(), True)
        , StructField("milliseconds",LongType(), True)
    ]
)

# COMMAND ----------

df_pit_stops = spark.read \
                    .format("json") \
                    .schema(schema_pit_stops) \
                    .option("multiline",True) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/pit_stops.json"
                    )

# COMMAND ----------

df_pit_stops = df_pit_stops \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id")

# COMMAND ----------

listPartitionFields=["race_id"]
df_pit_stops = func_AddAuditFields(df_pit_stops, varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import to_date, current_timestamp, col

# COMMAND ----------

if spark.catalog.tableExists("f1_processed.pit_stops"):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","True")
    
    dt_table = DeltaTable.forName(spark, "f1_processed.pit_stops")

    df_Temp = dt_table.toDF().filter("row_end >= to_date('9999-12-30','yyyy-mm-dd')").alias("tgt") \
                            .join(
                                df_pit_stops.alias("src"),
                                (col("tgt.stop") == col("src.stop"))
                                & (col("tgt.race_id") == col("src.race_id"))
                                & (col("tgt.driver_id") == col("src.driver_id"))
                                & (col("tgt.lap") == col("src.lap"))
                                & (
                                    (col("tgt.time") != col("src.time"))
                                    | (col("tgt.duration") != col("src.duration"))
                                    | (col("tgt.milliseconds") != col("src.milliseconds"))
                                )
                            ) \
                            .selectExpr(
                                "Null as SurrStop",
                                "Null as SurrRaceId",
                                "Null as SurrDriverId",
                                "Null as SurrLap",
                                "src.*"
                            )

    df_Temp = df_Temp.union(
        df_pit_stops.selectExpr(
            "stop as SurrStop",
            "race_id as SurrRaceId",
            "driver_id as SurrDriverId",
            "lap as SurrLap",
            "*"
        )
    )

    dt_table.alias("tgt").merge(
        df_Temp.alias("src"),
        "tgt.row_end >= to_date('9999-12-30','yyyy-MM-dd') \
        and tgt.stop = src.SurrStop \
        and tgt.driver_id = src.SurrDriverId \
        and tgt.race_id = src.SurrRaceId \
        and tgt.lap = src.SurrLap"
    ) \
    .whenMatchedUpdate(
        condition=(
            "tgt.time <> src.time \
            or tgt.duration <> src.duration \
            or tgt.milliseconds <> src.milliseconds"
        ),
        set={
            "row_end":current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_pit_stops.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy(listPartitionFields) \
        .saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.pit_stops
# MAGIC ZORDER BY (driver_id)

# COMMAND ----------

df_pit_stops.unpersist()
del df_pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops
