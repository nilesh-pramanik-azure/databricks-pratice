# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

df_races = spark.sql(f"""
                        select 
                        raceid as race_id
                        , year as race_year
                        , round
                        , circuitid as circuit_id
                        , name
                        , to_timestamp(date || ' ' || time, 'yyyy-MM-dd HH:mm:ss') as race_timestamp
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.races
                        """)

# COMMAND ----------

df_races.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for races") \
    .partitionBy("race_year") \
    .saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.races

# COMMAND ----------

df_races.unpersist()
