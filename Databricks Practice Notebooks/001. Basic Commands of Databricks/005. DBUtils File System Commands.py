# Databricks notebook source
# MAGIC %md
# MAGIC **dbutils.fs**

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

for pathValues in dbutils.fs.ls("/databricks-datasets/"):
    if pathValues.name.endswith("/"):
        print(f"Directory -- {pathValues.name}")
    else:
        print(f"File -- {pathValues.name}")

# COMMAND ----------


