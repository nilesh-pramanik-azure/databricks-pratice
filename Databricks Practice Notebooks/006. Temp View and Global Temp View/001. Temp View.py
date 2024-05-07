# Databricks notebook source
# MAGIC %md
# MAGIC #####Creating a data frame

# COMMAND ----------

df_sample = spark.createDataFrame(
    [
        ("E001","Arnab")
        ,("E002","Shankha")
        ,("E003","Aindrila")
    ]
).toDF("empid","empname")

# COMMAND ----------

df_sample.createOrReplaceTempView("v_t_sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_t_sample

# COMMAND ----------

spark.sql("""
          select * from v_t_sample
          """).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

df_sample.unpersist()
