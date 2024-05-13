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

schema_qualifying = StructType(
    [
        StructField("qualifyId", IntegerType(), True)
        , StructField("raceId", IntegerType(), True)
        , StructField("driverId", IntegerType(), True)
        , StructField("constructorId", IntegerType(), True)
        , StructField("number", IntegerType(), True)
        , StructField("position", IntegerType(), True)
        , StructField("q1", StringType(), True)
        , StructField("q2", StringType(), True)
        , StructField("q3", StringType(), True)
    ]
)

# COMMAND ----------

df_qualifying = spark.read \
                    .format("json") \
                    .schema(schema_qualifying) \
                    .option("multiline",True) \
                    .load(
                        path=f"/mnt/{varStorageAccountName}/{varBronzeContainerName}/{varCycleDate}/qualifying"
                    )

# COMMAND ----------

df_qualifying = df_qualifying \
    .withColumnRenamed("qualifyId","qualify_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id")

# COMMAND ----------

listPartitionFields=["race_id"]
df_qualifying = func_AddAuditFields(df_qualifying, varSourceSystem, varJobRunId, varCycleDate)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import to_date, current_timestamp, col

# COMMAND ----------

if spark.catalog.tableExists("f1_processed.qualifying"):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","True")
    
    dt_table = DeltaTable.forName(spark, "f1_processed.qualifying")

    df_Temp = dt_table.toDF().filter("row_end >= to_date('9999-12-30','yyyy-mm-dd')").alias("tgt") \
                            .join(
                                df_qualifying.alias("src"),
                                (col("tgt.qualify_id") == col("src.qualify_id"))
                                & (col("tgt.race_id") == col("src.race_id"))
                                & (col("tgt.driver_id") == col("src.driver_id"))
                                & (col("tgt.constructor_id") == col("src.constructor_id"))
                                & (
                                    (col("tgt.number") != col("src.number"))
                                    | (col("tgt.position") != col("src.position"))
                                    | (col("tgt.q1") != col("src.q1"))
                                    | (col("tgt.q2") != col("src.q2"))
                                    | (col("tgt.q3") != col("src.q3"))
                                )
                            ) \
                            .selectExpr(
                                "Null as SurrQualifyId",
                                "Null as SurrRaceId",
                                "Null as SurrDriverId",
                                "Null as SurrConstructorId",
                                "src.*"
                            )

    df_Temp = df_Temp.union(
        df_qualifying.selectExpr(
            "qualify_id as SurrQualifyId",
            "race_id as SurrRaceId",
            "driver_id as SurrDriverId",
            "constructor_id as SurrConstructorId",
            "*"
        )
    )

    dt_table.alias("tgt").merge(
        df_Temp.alias("src"),
        "tgt.row_end >= to_date('9999-12-30','yyyy-MM-dd') \
        and tgt.qualify_id = src.SurrQualifyId \
        and tgt.driver_id = src.SurrDriverId \
        and tgt.race_id = src.SurrRaceId \
        and tgt.constructor_id = src.SurrConstructorId"
    ) \
    .whenMatchedUpdate(
        condition=(
            "tgt.position <> src.position \
            or tgt.number <> src.number \
            or tgt.q1 <> src.q1 \
            or tgt.q2 <> src.q2 \
            or tgt.q3 <> src.q3"
        ),
        set={
            "row_end":current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_qualifying.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy(listPartitionFields) \
        .saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_processed.qualifying
# MAGIC ZORDER BY (qualify_id, driver_id, constructor_id)

# COMMAND ----------

df_qualifying.unpersist()
del df_qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying
