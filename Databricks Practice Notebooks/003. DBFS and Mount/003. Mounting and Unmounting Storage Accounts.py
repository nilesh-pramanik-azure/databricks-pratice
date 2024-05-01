# Databricks notebook source
# MAGIC %md
# MAGIC **Generalized way to Mount and Unmount Storage Account with the below Logic:**
# MAGIC - User wants to Mount
# MAGIC   - Container = "Not Mounted" Then Mount the Container
# MAGIC   - Container = "Mounted" Then throw a message "Mount Not Possible"
# MAGIC - User wants to Unmount
# MAGIC   - Container = "Mounted" Then unmount the Container
# MAGIC   - Container = "Not Mounted" Then throw a message "Unmount Not Possible"
# MAGIC
# MAGIC   **We will approach with this process by creating a User Defined Function (UDF) in a separate Folder:** 
# MAGIC   *"/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files"*
# MAGIC
# MAGIC   ** Then invoke the function from the external notebook inside this notebook with the help of an auxilliary command "%run"**

# COMMAND ----------

# MAGIC %md
# MAGIC **Let's invoke one of the Notebooks from Setup Files folder**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

print(varBrozeContainerName)

# COMMAND ----------

# MAGIC %md
# MAGIC **Now let is invoke the Funtion File from Setup Files Folder**

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/002. Mount or Unmount Storage"

# COMMAND ----------

dictMountDetails = {
    "clientID":varClientID
    , "clientSecret":varClientSecret
    , "tenantID":varTenantID
}

# COMMAND ----------

varStorageAccntName = varStorageAccntName

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varBrozeContainerName, "UnMOUNT")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varSilverContainerName, "UnMOUNT")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varGoldContainerName, "UnMOUNT")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varBrozeContainerName, "mount")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varSilverContainerName, "mount")

# COMMAND ----------

funcMountUnmountStorage(dictMountDetails, varStorageAccntName, varGoldContainerName, "mount")
