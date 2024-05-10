# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

df_results = spark.sql(f"""
                        select 
                        resultId as result_id
                        , raceId as race_id
                        , driverId as driver_id
                        , constructorId as constructor_id
                        , number
                        , grid
                        , position
                        , positionText as position_text
                        , positionOrder as position_order
                        , points
                        , laps
                        , time
                        , milliseconds
                        , fastestLap as fastest_lap
                        , rank
                        , fastestLapTime as fastest_lap_time
                        , fastestLapSpeed as fastest_lap_speed
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.results
                        """)

# COMMAND ----------

df_results.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for results") \
    .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.results

# COMMAND ----------

df_results.unpersist()
