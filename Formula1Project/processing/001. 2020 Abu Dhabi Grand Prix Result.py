# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

df_races = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/races"
)

# COMMAND ----------

df_circuits = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/circuits"
)

# COMMAND ----------

df_drivers = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/drivers"
)

# COMMAND ----------

df_constructors = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/constructors"
)

# COMMAND ----------

df_results = spark.read.format("parquet").load(
    path=f"/mnt/{varStorageAccntName}/{varSilverContainerName}/results"
)

# COMMAND ----------

from pyspark.sql.functions import date_format, col, lit, current_timestamp, desc, asc

# COMMAND ----------

df_AbuDhabiCircuit = df_races.join(
                        df_circuits,
                        (df_races["circuit_id"] == df_circuits.circuit_id)
                        & (df_races.race_year == 2020)
                        & (date_format(df_races.race_timestamp,"yyyy-MM-dd") == "2020-12-13"),
                        "inner"
                    ).join(
                        df_results,
                        df_results.race_id == df_races.race_id,
                        "inner"
                    ).join(
                        df_drivers,
                        df_drivers.driver_id == df_results.driver_id,
                        "inner"
                    ).join(
                        df_constructors,
                        df_constructors.contructor_id == df_results.constructor_id,
                        "inner"
                    ) \
                    .select(
                        df_races.race_year
                        , df_races.name.alias("race_name")
                        , date_format(df_races.race_timestamp,"yyyy-MM-dd").alias("race_date")
                        , df_circuits.location.alias("circuit_location")
                        , df_drivers.name.alias("driver_name")
                        , df_drivers.number.alias("driver_number")
                        , df_drivers.nationality.alias("driver_nationality")
                        , df_constructors.name.alias("team")
                        , df_results.grid
                        , df_results.fastest_lap_time.alias("fastest lap")
                        , df_results.time.alias("race time")
                        , df_results.points
                        , current_timestamp().alias("created_date")
                    ) \
                    .orderBy(
                        desc(df_results.points),
                        df_constructors.name.asc(),
                        asc(df_drivers.name)
                    )

# COMMAND ----------

display(df_AbuDhabiCircuit)

# COMMAND ----------

df_AbuDhabiCircuit.write.format("parquet").mode("overwrite").save(
    path=f"/mnt/{varStorageAccntName}/{varGoldContainerName}/race_results"
)

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{varStorageAccntName}/{varGoldContainerName}/race_results"))
