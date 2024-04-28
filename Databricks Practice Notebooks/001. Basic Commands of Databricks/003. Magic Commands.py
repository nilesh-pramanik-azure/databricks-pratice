# Databricks notebook source
# MAGIC %md
# MAGIC #####This Notebook is of Python Language. Will be using Magic Command to switch to SQL language.

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing a Python Code to print even numbers between 1 to 20**

# COMMAND ----------

for val in range(1,21):
    if val % 2 == 0:
        print(f"{val} is Even")

# COMMAND ----------

# MAGIC %md
# MAGIC **Switching to SQL in the Python File using Magic Command**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'This is SQL Code' AS Field1
