# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

df_pit_stops = spark.sql(f"""
                        select 
                        raceId as race_id
                        , driverId as driver_id
                        , stop
                        , lap
                        , time
                        , duration
                        , milliseconds
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.pit_stops
                        """)

# COMMAND ----------

df_pit_stops.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for pit_stops") \
    .saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.pit_stops

# COMMAND ----------

df_pit_stops.unpersist()
