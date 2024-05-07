# Databricks notebook source
# MAGIC %md
# MAGIC #####Group By Function
# MAGIC **Aggregate Functions are used to fetch only one row based on the total data set**<br>
# MAGIC **But Group By will be used to fetch based on grouped data - fetching multiple rows** 

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

from pyspark.sql.functions import max, min, count, sum, expr,col

# COMMAND ----------

df_sample \
.groupBy(df_sample.deptid) \
.sum("salary") \
.select(expr('*')).show()

# COMMAND ----------

df_sample \
.groupBy(df_sample.deptid) \
.sum("salary") \
.withColumnRenamed("sum(salary)","SalaryDept").show()

# COMMAND ----------

df_sample \
.groupBy(
    df_sample.deptid
) \
.agg(
    sum(df_sample["salary"]).alias("salaryDept")
    , min(df_sample.salary).alias("MinDeptSalary")
    , max(col("salary")).alias("MaxDeptSalary")
    , count(expr("*")).alias("RecordCount")
    , count(df_sample.empname).alias("NotNullEmpNameCount")
).show()
