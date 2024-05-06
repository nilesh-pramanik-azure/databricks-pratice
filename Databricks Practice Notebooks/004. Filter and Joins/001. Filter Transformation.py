# Databricks notebook source
# MAGIC %md
# MAGIC #####Create temporary Data Frames by using spark functions
# MAGIC **Use those dataframes to show the filter operation**

# COMMAND ----------

df_emptable = spark.createDataFrame(
    [
        ("E001","JOHN",5000.75,"DEP01")
        , ("E002","JOHN",6000.75,"DEP01")
        , ("E003","JOHN",7000.00,"DEP02")
    ]
).toDF("EMPID","EMPNAME","SAL","DEPTID")

# COMMAND ----------

df_depttable = spark.createDataFrame(
    [
        ("DEP01","ACCOUNTANCY")
        , ("DEP02","COMPUTER IT")
        , ("DEP03", "SALES JOB")
    ], schema=["DEPTID","DEPTNAME"]
)

# COMMAND ----------

df_desigtable = spark.createDataFrame(
    [
        ("E001","MANAGER"), ("E002","ASSOCIATE")
    ]
).toDF("EMPID", "DESIGNATION")

# COMMAND ----------

display(df_emptable)

# COMMAND ----------

display(df_depttable)

# COMMAND ----------

display(df_desigtable)

# COMMAND ----------

# MAGIC %md
# MAGIC **belongs to DEP01**

# COMMAND ----------

df_emptable.filter("deptid = 'DEP01'").show()

# COMMAND ----------

df_emptable.filter(df_emptable.DEPTID == 'DEP01').show()

# COMMAND ----------

df_emptable.filter(df_emptable["DEPTID"] == 'DEP01').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_emptable.filter(col("DEPTID") == 'DEP01').show()

# COMMAND ----------

# MAGIC %md
# MAGIC **EMPLOYEES OF DEP01 AND SALARY > 5500**

# COMMAND ----------

df_emptable.filter("DEPTID = 'DEP01' AND SAL > 5500").show()

# COMMAND ----------

df_emptable.filter((df_emptable.DEPTID == 'DEP01') & (df_emptable.SAL > 5500)).show()

# COMMAND ----------

df_emptable.filter((df_emptable["DEPTID"] == 'DEP01') & (df_emptable["SAL"] > 5500)).show()

# COMMAND ----------

df_emptable.filter((col("DEPTID") == 'DEP01') & (col("SAL") > 5500)).show()
