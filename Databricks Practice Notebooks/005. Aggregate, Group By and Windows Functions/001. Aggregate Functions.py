# Databricks notebook source
# MAGIC %md
# MAGIC #####Creating some sample dataframe to test the aggregate functions

# COMMAND ----------

df_sample = spark.createDataFrame(
    [
        ("E001","Shankha",1000,"D001")
        , ("E002","Arnab",2000,"D001")
        , ("E003", "Aindrila", 1000,"D002")
        , ("E004", None, 7000, "D003")
    ], schema=["empid","empname","salary","deptid"]
)

# COMMAND ----------

from pyspark.sql.functions import count,max, min, avg, sum, sumDistinct, countDistinct;

# COMMAND ----------

df_sample.select(count("*")).show()

# COMMAND ----------

df_sample.select(
    count("*").alias("CountAll")
    , count(df_sample.empname).alias("CountName")
    , countDistinct(df_sample.deptid).alias("CountName").alias("CountDistinctName")
    , sum(df_sample.salary).alias("SumofSalary")
    , sumDistinct(df_sample.salary).alias("SumDistinctofSalary")
    , min(df_sample.salary).alias("MinSalary")
    , max(df_sample.salary).alias("MaxSalary")
    , avg(df_sample.salary).alias("AvgSalary")
).show()
