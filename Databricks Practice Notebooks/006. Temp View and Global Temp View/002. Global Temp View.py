# Databricks notebook source
df_sample = spark.createDataFrame(
    [
        ("E001","Arnab")
        ,("E002","Shankha")
        ,("E003","Aindrila")
    ]
).toDF("empid","empname")

# COMMAND ----------

df_sample.createOrReplaceGlobalTempView("v_gt_sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.v_gt_sample

# COMMAND ----------

spark.sql("""
          select * from global_temp.v_gt_sample
          """).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

display(spark.table("global_temp.v_gt_sample").cache)


# COMMAND ----------

display(spark.table("global_temp.v_gt_sample").count)

# COMMAND ----------

spark.catalog.uncacheTable("global_temp.v_gt_sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

display(spark.table("global_temp.v_gt_sample").cache)

# COMMAND ----------

display(spark.table("global_temp.v_gt_sample").count)

# COMMAND ----------

df_sample.unpersist()
