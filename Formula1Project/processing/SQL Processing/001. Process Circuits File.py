# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.circuits

# COMMAND ----------

df_circuits = spark.sql(f"""
                        select 
                        circuitid as circuit_id
                        , circuitref as circuit_ref
                        , name
                        , location
                        , country
                        , lat as latitude
                        , lng as longitude
                        , alt as altitude
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.circuits
                        """)

# COMMAND ----------

df_circuits.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for circuit") \
    .saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.circuits
