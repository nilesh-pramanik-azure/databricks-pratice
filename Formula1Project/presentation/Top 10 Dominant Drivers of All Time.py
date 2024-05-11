# Databricks notebook source
htmlText = """<h1 style="text-align:center; color:Teal">Top 10 Dominant Drivers of all time with minimum 50 races</h1>"""
displayHTML(htmlText)

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

df_results = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/results"
)

# COMMAND ----------

df_drivers = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/drivers"
)

# COMMAND ----------

from pyspark.sql.functions import avg, when, sum, count, desc, asc, rank, round

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

win_dominant_drivers = Window.orderBy(desc("avg_uniform_points"), desc("total_wins"))

# COMMAND ----------

df_dominant_drivers_all_time = df_drivers.join(
    df_results,
    df_drivers.driver_id == df_results.driver_id,
    "inner"
) \
.withColumn("uniform_points",
            when(df_results.position == 1, 10) \
            .when(df_results.position == 2, 9) \
            .when(df_results.position == 3, 8) \
            .when(df_results.position == 4, 7) \
            .when(df_results.position == 5, 6) \
            .when(df_results.position == 6, 5) \
            .when(df_results.position == 7, 4) \
            .when(df_results.position == 8, 3) \
            .when(df_results.position == 9, 2) \
            .when(df_results.position == 10, 1) \
            .otherwise(0).alias("uniform_points")    
            ) \
.groupBy(
    df_drivers.driver_id
    , df_drivers.name
    , df_drivers.nationality
) \
.agg(
    sum(df_results.points).alias("total_points_actual")
    , sum("uniform_points").alias("total_points_uniform")
    , round(avg("uniform_points"), 2).alias("avg_uniform_points")
    , count(when(df_results.position == 1, True)).alias("total_wins")
    , count('*').alias("total_races")
) \
.filter(
    "total_races >= 50"
) \
.withColumn("driver_rank", rank().over(win_dominant_drivers)) \
.filter("driver_rank <= 10") \
.select(
    df_drivers.name.alias("driver_name")
    , df_drivers.nationality
    , "total_points_actual"
    , "total_points_uniform"
    , "avg_uniform_points"
    , "total_wins"
    , "total_races"
    , "driver_rank"
) \
.orderBy(asc("driver_rank"))


# COMMAND ----------

display(df_dominant_drivers_all_time)

# COMMAND ----------

df_dominant_drivers_all_time.write.format("parquet").mode("overwrite").saveAsTable("f1_presentation.dominant_drivers")

# COMMAND ----------

df_dominant_drivers_all_time.unpersist()
df_drivers.unpersist()
df_results.unpersist()
del df_results
del df_dominant_drivers_all_time
del df_drivers

# COMMAND ----------

# https://adb-3136375867653593.13.azuredatabricks.net/?o=3136375867653593#notebook/757920083722599/dashboard/757920083722654/present
