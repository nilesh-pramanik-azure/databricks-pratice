# Databricks notebook source
pathRepoRootFolder="/Workspace/Repos/Databricks-Certification/databricks-pratice/Databricks Practice Notebooks"

# COMMAND ----------

varStorageAccntName="datalakedbpractice"
varBrozeContainerName="bronze"
varSilverContainerName="silver"
varGoldContainerName="gold"

# COMMAND ----------

varClientID=dbutils.secrets.get("databricks-practice-certification","clientID")
varTenantID=dbutils.secrets.get("databricks-practice-certification","newTenantID")
varClientSecret=dbutils.secrets.get("databricks-practice-certification","clientSecret")
