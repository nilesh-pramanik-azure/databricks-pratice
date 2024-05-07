# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

df_results = spark.read \
            .format("parquet") \
            .load(
                path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/results"
            )

# COMMAND ----------

df_races = spark.read \
            .format("parquet") \
            .load(
                path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/races"
            )

# COMMAND ----------

df_constructors = spark.read \
                .format("parquet") \
                .load(
                    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/constructors"
                )

# COMMAND ----------

from pyspark.sql.functions import sum, count_if, count, expr, col, when, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

df_requiredAttributes = df_races.filter(df_races.race_year == 2020) \
                        .join(
                            df_results,
                            df_races.race_id == df_results.race_id,
                            "inner"
                        ) \
                        .join(
                            df_constructors,
                            df_results.constructor_id == df_constructors.contructor_id,
                            "inner"
                        ) \
                        .select(
                            df_races.race_year,
                            df_constructors.name.alias("team"),
                            df_results.points,
                            df_results.position
                        )

# COMMAND ----------

constructorWindow = Window.partitionBy(df_requiredAttributes.race_year).orderBy(desc("total_points"), desc(col("num_of_wins")))

# COMMAND ----------

df_constructorStandings = df_requiredAttributes \
    .groupBy(
        df_requiredAttributes.race_year,
        df_requiredAttributes.team
    ) \
    .agg(
        sum(df_requiredAttributes.points).alias("total_points"),
        count_if(df_requiredAttributes.position == 1).alias("num_of_wins")
    ) \
    .withColumn("constructor_rank", rank().over(constructorWindow)) \
    .orderBy(desc(col("total_points")), desc(col("num_of_wins"))) \
    .select(
        df_requiredAttributes["race_year"]
        , df_requiredAttributes["team"]
        , col("total_points")
        , "num_of_wins"
        , "constructor_rank"
    )

# COMMAND ----------

df_constructorStandings.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(
        path=f"/mnt/{varStorageAccntName}/{varGoldContainerName}/constructor_standings"
    )

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{varStorageAccntName}/{varGoldContainerName}/constructor_standings"))

# COMMAND ----------

df_results.unpersist()
df_races.unpersist()
df_constructors.unpersist()
df_requiredAttributes.unpersist()
df_constructorStandings.unpersist()
