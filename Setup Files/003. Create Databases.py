# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Databricks-Certification/databricks-pratice/Setup Files/001. Paths, Variables and Credentials"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_raw cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_processed cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_presentation cascade

# COMMAND ----------

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS f1_raw
COMMENT 'This database will hold all the tables from raw files'
LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}/f1_raw_database'
""")

# COMMAND ----------

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS f1_processed
COMMENT 'This database will hold all the tables for processed files and the tables will be Managed'
LOCATION '/mnt/{varStorageAccntName}/{varSilverContainerName}/SQL/f1_processed_database'
""")

# COMMAND ----------

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS f1_presentation
COMMENT 'This database will hold all the tables for processed files and the tables will be Managed'
LOCATION '/mnt/{varStorageAccntName}/{varGoldContainerName}/SQL/f1_presentation_database'
""")

# COMMAND ----------

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS deltadb
COMMENT 'This database will hold all the tables for processed files and the tables will be Managed'
LOCATION '/mnt/{varStorageAccntName}/{varBrozeContainerName}'
""")
