# Databricks notebook source
# MAGIC %md
# MAGIC #####Mounting the storage containers
# MAGIC - Mount the Bronze for reading the raw files
# MAGIC - Mount the Silver for storing the processed raw files

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/002. Mount or Unmount Storage"

# COMMAND ----------

dictMountDetails = {
    "clientID":varClientID
    , "clientSecret":varClientSecret
    , "tenantID":varTenantID
}

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varBrozeContainerName, "mount")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varSilverContainerName, "mount")
