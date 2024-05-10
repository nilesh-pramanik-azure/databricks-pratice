# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

dbutils.widgets.text("source_system","")
dbutils.widgets.text("job_id","11")
varSourceSystem = dbutils.widgets.get("source_system")
varJobId=dbutils.widgets.get("job_id")

# COMMAND ----------

df_constructors = spark.sql(f"""
                        select 
                        constructorId as constructor_id
                        , constructorRef as constructor_ref
                        , name
                        , nationality
                        , current_timestamp() as ingestion_dt
                        , '{varSourceSystem}' as source_system
                        , int({varJobId}) as job_id
                        from
                        f1_raw.constructors
                        """)

# COMMAND ----------

df_constructors.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("comment","This is a managed table for constructors") \
    .saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended f1_processed.constructors

# COMMAND ----------

df_constructors.unpersist()
