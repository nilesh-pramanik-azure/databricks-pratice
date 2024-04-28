# Databricks notebook source
# MAGIC %md
# MAGIC #####Accessing Storage Account by Service Principle
# MAGIC * Create the Service Principle<br>
# MAGIC * Get the Service Principle Tokens of Client (Application), Directory (Tenant) and Secret<br>
# MAGIC * Provide the **"Storage Blob Data Contributor"** Role to the Service Principle<br>
# MAGIC * Set the spark configuration to setup the access<br>
# MAGIC * List the files available in the container "bronze"<br>
# MAGIC * Read the file "circuit.csv"
# MAGIC * Read one more file from the sub-folder

# COMMAND ----------

# MAGIC %md
# MAGIC **Assigning the Service Principle Tokens to a Local Variables**

# COMMAND ----------

client_id="ef91e7cc-9ab7-428f-8dd6-04fca9953f70"
tenant_id="88b81850-f835-4d8a-869c-97cc6c0f8e64"
secret_key="HIi8Q~LQYqgU_DzhvENJum6nVue5aPojhRzFJa4b"

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting up Spark Configuration**

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakedbpractice.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakedbpractice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakedbpractice.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakedbpractice.dfs.core.windows.net", secret_key)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakedbpractice.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Container Bronze**

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@datalakedbpractice.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying circuits.csv**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC **List the files in the Sub-Folder**

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times"

# COMMAND ----------

# MAGIC %md
# MAGIC **Display a file from Sub-Folder**

# COMMAND ----------

display(spark.read.csv("abfss://bronze@datalakedbpractice.dfs.core.windows.net/lap_times/lap_times_split_1.csv"))
