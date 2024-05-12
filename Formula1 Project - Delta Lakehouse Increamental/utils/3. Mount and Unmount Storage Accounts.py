# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/1. Variables and Paths"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Formula1 Project - Delta Lakehouse Increamental/utils/4. Common UDFs"

# COMMAND ----------

dictSecretKeys = {
    "clientId": dbutils.secrets.get("databricks-practice-certification","clientID"),
    "clientSecret": dbutils.secrets.get("databricks-practice-certification","clientSecret"),
    "tenantId": dbutils.secrets.get("databricks-practice-certification", "newTenantId")
}

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"bronze",dictSecretKeys,"unmount")

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"silver",dictSecretKeys,"unmount")

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"gold",dictSecretKeys,"unmount")

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"bronze",dictSecretKeys,"mount")

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"silver",dictSecretKeys,"mount")

# COMMAND ----------

func_MountUnmountContainer(varStorageAccountName,"gold",dictSecretKeys,"mount")
