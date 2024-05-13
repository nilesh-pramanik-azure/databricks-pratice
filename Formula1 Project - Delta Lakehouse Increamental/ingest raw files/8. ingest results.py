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

schema_results = StructType(
    [
        StructField("resultId",IntegerType(),True)
        , StructField("raceId",IntegerType(),True)
        , StructField("driverId",IntegerType(),True)
        , StructField("constructorId",IntegerType(),True)
        , StructField("number",IntegerType(),True)
        , StructField("grid",IntegerType(),True)
        , StructField("position",IntegerType(),True)
        , StructField("positionText",StringType(),True)
        , StructField("positionOrder",IntegerType(),True)
        , StructField("points",IntegerType(),True)
        , StructField("laps",IntegerType(),True)
        , StructField("time",StringType(),True)
        , StructField("milliseconds",LongType(),True)
        , StructField("fastestLap",IntegerType(),True)
        , StructField("rank",IntegerType(),True)
        , StructField("fastestLapTime",StringType(),True)
        , StructField("fastestLapSpeed",DoubleType(),True)
        , StructField("statusId",IntegerType(),True)
    ]
)

# COMMAND ----------

df_results = spark.read \
                    .format("json") \
                    .schema(schema_results) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/results.json"
                    )

# COMMAND ----------

df_results = df_results \
    .withColumnRenamed("resultId","result_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .drop("statusId")

# COMMAND ----------

listPartitionFields=["race_id"]
df_results = func_AddAuditFields(df_results, varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import to_date, current_timestamp, col

# COMMAND ----------

if spark.catalog.tableExists("f1_processed.results"):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","True")
    
    dt_table = DeltaTable.forName(spark, "f1_processed.results")

    df_Temp = dt_table.toDF().filter("row_end >= to_date('9999-12-30','yyyy-mm-dd')").alias("tgt") \
                            .join(
                                df_results.alias("src"),
                                (col("tgt.result_id") == col("src.result_id"))
                                & (col("tgt.race_id") == col("src.race_id"))
                                & (col("tgt.driver_id") == col("src.driver_id"))
                                & (col("tgt.constructor_id") == col("src.constructor_id"))
                                & (
                                    (col("tgt.number") != col("src.number"))
                                    | (col("tgt.position") != col("src.position"))
                                    | (col("tgt.grid") != col("src.grid"))
                                    | (col("tgt.position_text") != col("src.position_text"))
                                    | (col("tgt.position_order") != col("src.position_order"))
                                    | (col("tgt.points") != col("src.points"))
                                    | (col("tgt.laps") != col("src.laps"))
                                    | (col("tgt.time") != col("src.time"))
                                    | (col("tgt.milliseconds") != col("src.milliseconds"))
                                    | (col("tgt.fastest_lap") != col("src.fastest_lap"))
                                    | (col("tgt.rank") != col("src.rank"))
                                    | (col("tgt.fastest_lap_time") != col("src.fastest_lap_time"))
                                    | (col("tgt.fastest_lap_speed") != col("src.fastest_lap_speed"))
                                )
                            ) \
                            .selectExpr(
                                "Null as SurrResultId",
                                "Null as SurrRaceId",
                                "Null as SurrDriverId",
                                "Null as SurrConstructorId",
                                "src.*"
                            )

    df_Temp = df_Temp.union(
        df_results.selectExpr(
            "result_id as SurrResultId",
            "race_id as SurrRaceId",
            "driver_id as SurrDriverId",
            "constructor_id as SurrConstructorId",
            "*"
        )
    )

    dt_table.alias("tgt").merge(
        df_Temp.alias("src"),
        "tgt.row_end >= to_date('9999-12-30','yyyy-MM-dd') \
        and tgt.result_id = src.SurrResultId \
        and tgt.driver_id = src.SurrDriverId \
        and tgt.race_id = src.SurrRaceId \
        and tgt.constructor_id = src.SurrConstructorId"
    ) \
    .whenMatchedUpdate(
        condition=(
            "tgt.position <> src.position \
            or tgt.number <> src.number \
            or tgt.grid <> src.grid \
            or tgt.position_text <> src.position_text \
            or tgt.position_order <> src.position_order \
            or tgt.points <> src.points \
            or tgt.laps <> src.laps \
            or tgt.time <> src.time \
            or tgt.milliseconds <> src.milliseconds \
            or tgt.fastest_lap <> src.fastest_lap \
            or tgt.rank <> src.rank \
            or tgt.fastest_lap_time <> src.fastest_lap_time \
            or tgt.fastest_lap_speed <> src.fastest_lap_speed"
        ),
        set={
            "row_end":current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_results.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy(listPartitionFields) \
        .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.results
# MAGIC ZORDER BY (result_id, driver_id, constructor_id)

# COMMAND ----------

df_results.unpersist()
del df_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results
