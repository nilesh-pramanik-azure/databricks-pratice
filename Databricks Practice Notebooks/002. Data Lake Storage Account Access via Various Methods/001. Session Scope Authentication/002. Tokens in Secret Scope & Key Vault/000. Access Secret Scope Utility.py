# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing the DBUtils Secrets and looking in all the functions
# MAGIC #####Secret Scope "databricks-practice-certification" has been created

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying the List of Scopes in this Databricks Workspace**

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

# MAGIC %md
# MAGIC **Listing the contents of scope "databricks-practice-certification"**

# COMMAND ----------

display(dbutils.secrets.list("databricks-practice-certification"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetching the value of "storageAccessKey" from scope "databricks-practice-certification"**<br>
# MAGIC - It will give REDACTED value - which means hidden as its a key

# COMMAND ----------

display(dbutils.secrets.get("databricks-practice-certification","storageAccessKey"))

# COMMAND ----------

display(dbutils.secrets.getBytes("databricks-practice-certification","storageAccessKey"))
