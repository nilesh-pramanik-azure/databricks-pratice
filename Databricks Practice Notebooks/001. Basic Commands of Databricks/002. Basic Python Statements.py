# Databricks notebook source
# MAGIC %md
# MAGIC **Basic Print Statements**

# COMMAND ----------

print("Hello World!!!")

# COMMAND ----------

# MAGIC %md
# MAGIC **Print the values of Variable in two different ways**

# COMMAND ----------

varFirstName="Arnab"
varLastName="Desarkar"

print(f"My firstname is {varFirstName} and lastname is {varLastName}")
print("My firstname is {f} and lastname is {l}".format(f=varFirstName, l=varLastName))

# COMMAND ----------

# MAGIC %md
# MAGIC **For Loop to print from 1 to 10**

# COMMAND ----------

for i in range(1,11):
    print(f"Value of I = {i}")

# COMMAND ----------


