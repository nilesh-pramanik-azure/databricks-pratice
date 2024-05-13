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

df_drivers = spark.read \
            .format("parquet") \
            .load(
                path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/drivers"
            )

# COMMAND ----------

df_constructors = spark.read \
                .format("parquet") \
                .load(
                    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/constructors"
                )

# COMMAND ----------

df_requiredAttributes = df_races.filter(df_races.race_year == 2020).join(
                            df_results,
                            df_races.race_id == df_results.race_id,
                            "inner"
                    ) \
                    .join(
                        df_drivers,
                        df_results.driver_id == df_drivers.driver_id,
                        "inner"
                    ) \
                    .join(
                        df_constructors,
                        df_results.constructor_id == df_constructors.contructor_id,
                        "inner"
                    ) \
                    .select(
                        df_races.race_year
                        , df_drivers.driver_id
                        , df_drivers.name.alias("driver_name")
                        , df_drivers.nationality.alias("driver_nationality")
                        , df_constructors.name.alias("team")
                        , df_results.position
                        , df_results.points
                    )

# COMMAND ----------

from pyspark.sql.functions import sum, count_if, count, expr, col, when, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

driverStandingWindow = Window.partitionBy(df_requiredAttributes.race_year).orderBy(desc("total_points"), desc("num_of_wins"))

# COMMAND ----------

df_driverStandings = df_requiredAttributes \
    .groupBy(
        df_requiredAttributes.race_year
        , df_requiredAttributes.driver_id
        , df_requiredAttributes.driver_name
        , df_requiredAttributes.driver_nationality
        , df_requiredAttributes.team
    ) \
    .agg(
        sum(df_requiredAttributes.points).alias("total_points"),
        count_if(df_requiredAttributes.position == 1).alias("num_of_wins"),
        count(when(df_requiredAttributes.position == 1, True)).alias("num_of_wins_V2")
    ) \
    .withColumn("driver_rank", rank().over(driverStandingWindow)) \
    .orderBy(df_requiredAttributes.race_year.desc(), col("driver_rank").asc()) \
    .select(
        df_requiredAttributes["race_year"]
        , df_requiredAttributes["driver_name"]
        , df_requiredAttributes["driver_nationality"]
        , df_requiredAttributes["team"]
        , col("total_points")
        , col("num_of_wins")
        , col("driver_rank")
    )

# COMMAND ----------

df_driverStandings.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(
        path=f"/mnt/{varStorageAccntName}/{varGoldContainerName}/driver_standings"
    )

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{varStorageAccntName}/{varGoldContainerName}/driver_standings"))
