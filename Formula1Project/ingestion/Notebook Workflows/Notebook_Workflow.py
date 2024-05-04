# Databricks notebook source
# MAGIC %md
# MAGIC #####This note book will chain all the ingestion workflows one after the another

# COMMAND ----------

varSource="Ergast API"
varJobId=999999

# COMMAND ----------

dict_parameter = {"source":varSource, "jobid":varJobId}

# COMMAND ----------

listIngestionFileNames=["1. ingest_circuits_file",
                        "2. ingest_races_file"
                        ,"3. ingest_constructor_file"
                        ,"4. ingest_drivers_file"
                        ,"5. ingest_results_file"
                        ,"6. ingest_pit_stops_file"
                        ,"7. ingest_lap_times_folder"
                        ,"8. ingest_qualifying_folder"]

# COMMAND ----------

varIngestionFilePath="/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1Project/ingestion/Notebook Workflows"

# COMMAND ----------

for varFileName in listIngestionFileNames:
    varJobStatus=dbutils.notebook.run(f"{varIngestionFilePath}/{varFileName}", 0, dict_parameter)

    if varJobStatus == "Success":
        print(f"SUCCESS: {varFileName}")
    else:
        print(f"ERROR: {varFileName}")
        print("No other Notebooks will be executed.... Exiting the Program.")
        break
