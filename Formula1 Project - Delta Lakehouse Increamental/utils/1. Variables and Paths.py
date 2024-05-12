# Databricks notebook source
# MAGIC %md
# MAGIC **Storage Account Variable Declarations**

# COMMAND ----------

varStorageAccountName="datalakedbpractice"
varBronzeContainerName="bronze"
varSilverContainerName="silver"
varGoldContainerName="gold"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Getting the Secret Details from Secret Scope

# COMMAND ----------

varClientID=dbutils.secrets.get("databricks-practice-certification","clientID")
varTenantID=dbutils.secrets.get("databricks-practice-certification","tenantID")
varClientSecret=dbutils.secrets.get("databricks-practice-certification","clientSecret")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Full Paths of UDF Notebook

# COMMAND ----------

varPathUDFNotebook="/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"
