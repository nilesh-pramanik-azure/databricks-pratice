# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

df_qualifying = spark.sql(f"""
                        select 
                        qualifyId as qualify_id
                        , raceId as race_id
                        , driverId as driver_id
                        , constructorId as constructor_id
                        , number
                        , position
                        , q1
                        , q2
                        , q3
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.qualifying
                        """)

# COMMAND ----------

df_qualifying.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for qualifying") \
    .saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.qualifying

# COMMAND ----------

df_qualifying.unpersist()
