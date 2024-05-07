# Databricks notebook source
# MAGIC %md
# MAGIC #####Use of Window Function with Group by and Aggregate Functions

# COMMAND ----------

df_sample = spark.createDataFrame(
    [
        ("E001","Shankha",1000,"D001","2024-01-10")
        , ("E002","Arnab",2000,"D001","2023-01-10")
        , ("E003", "Aindrila", 1000,"D002","2024-02-10")
        , ("E004", None, 7000, "D003","2024-05-10")
    ], schema=["empid","empname","salary","deptid","hiredate"]
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, lead, lag, desc, asc, max, to_date

# COMMAND ----------

salWindow = Window.partitionBy(df_sample.deptid).orderBy(desc(df_sample.salary))
dateWindow = Window.partitionBy(df_sample.deptid).orderBy(asc(df_sample.hiredate))

# COMMAND ----------

df_sample \
            .withColumn("SalaryRank", rank().over(salWindow)) \
            .withColumn("Current Hire Date", df_sample.hiredate) \
            .withColumn("Previous Hire Date", lag(df_sample.hiredate).over(dateWindow)) \
            .show()

# COMMAND ----------

df_sample \
            .withColumn("SalaryRank", rank().over(salWindow)) \
            .withColumn("Current Hire Date", df_sample.hiredate) \
            .withColumn("Next Hire Date", lead(df_sample.hiredate).over(dateWindow)) \
            .show()

# COMMAND ----------

df_sample \
            .withColumn("SalaryRank", rank().over(salWindow)) \
            .withColumn("Current Hire Date", df_sample.hiredate) \
            .withColumn("Next Hire Date", lead(df_sample.hiredate).over(dateWindow)) \
            .show()

# COMMAND ----------

df_sample \
            .withColumn("SalaryRank", rank().over(salWindow)) \
            .withColumn("Current Hire Date", df_sample.hiredate) \
            .withColumn("Next Hire Date", lead(df_sample.hiredate).over(dateWindow)) \
            .groupBy(
                df_sample.deptid
            ) \
            .agg(
                max(to_date("Next Hire Date",'yyyy-MM-dd')).alias("Max Date")
            ).show()
