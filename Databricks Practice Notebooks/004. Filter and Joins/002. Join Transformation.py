# Databricks notebook source
# MAGIC %md
# MAGIC #####Create temporary Data Frames by using spark functions
# MAGIC **Use those dataframes to show the join operation**

# COMMAND ----------

df_emptable = spark.createDataFrame(
    [
        ("E001","JOHN",5000.75,"DEP01")
        , ("E002","JOHN",6000.75,"DEP01")
        , ("E003","JOHN",7000.00,"DEP02")
        , ("E004","JOHN",9000.00,"DEP10")
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

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #####INNER JOIN - DEFAULT JOIN

# COMMAND ----------

df_emptable.join(df_depttable,
                 df_emptable.DEPTID == df_depttable.DEPTID,
                 "inner"
                 ) \
                .select(
                    col("EMPID")
                    , df_depttable.DEPTID
                    , col('DEPTNAME')
                ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####LEFT JOIN

# COMMAND ----------

df_depttable.join(df_emptable
                                , df_depttable.DEPTID == df_emptable.DEPTID
                                , "left"                                
                                ) \
                                .select(
                                    df_depttable.DEPTID,
                                    df_depttable.DEPTNAME,
                                    df_emptable.EMPID
                                ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####RIGHT JOIN - WHERE SAL > 5500

# COMMAND ----------

df_depttable.join(df_emptable
                   , (df_depttable.DEPTID == df_emptable.DEPTID) & (df_emptable.SAL > 5500)
                   , "rightouter"
                   ) \
                    .select(
                        df_depttable.DEPTID
                        , df_depttable.DEPTNAME
                        , df_emptable.EMPID
                        , df_emptable.EMPNAME
                    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####FULL OUTER JOIN

# COMMAND ----------

df_emptable.join(
    df_depttable,
    df_emptable.DEPTID == df_depttable.DEPTID,
    "full"
) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####SEMI JOIN
# MAGIC **Its basically an inner join with the containts of only LEFT table... RIGHT table cannot be accessed.**

# COMMAND ----------

#Only the Left Table containts displayed
df_emptable.join(
    df_depttable,
    df_emptable.DEPTID == df_depttable.DEPTID,
    "semi"
) \
.show()

# COMMAND ----------

#Accessing Right Table will fail
df_emptable.join(
    df_depttable,
    df_emptable.DEPTID == df_depttable.DEPTID,
    "semi"
) \
.select(
    df_depttable.DEPTID
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####ANTI-SEMI JOIN
# MAGIC **Its basically an inner join with the containts of only LEFT table (WHICH ARE NOT MATCHED)... RIGHT table cannot be accessed.**

# COMMAND ----------

#Only the Left Table containts displayed
df_emptable.join(
    df_depttable,
    df_emptable.DEPTID == df_depttable.DEPTID,
    "anti"
) \
.show()

# COMMAND ----------

#Accessing Right Table will fail
df_emptable.join(
    df_depttable,
    df_emptable.DEPTID == df_depttable.DEPTID,
    "leftanti"
) \
.select(
    df_depttable.DEPTID
).show()
