# Databricks notebook source
# MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Delete Previous Data in container

# COMMAND ----------

if (dbutils.fs.rm(f"{cleanedFiles_file_path}payment_details", True)):
    print("Cleaned payment details data")
else:
    print("No Data present to delete")

# COMMAND ----------

if(dbutils.fs.rm(f"{cleanedFiles_file_path}loan_details", True)):
    print("Cleaned loan details data")
else:
    print("No Data present to delete")

# COMMAND ----------

if(dbutils.fs.rm(f"{cleanedFiles_file_path}loan_defaulters", True)):
    print("Cleaned loan defaulters details data")
else:
    print("No Data present to delete")

# COMMAND ----------

if(dbutils.fs.rm(f"{cleanedFiles_file_path}investor_loan_details", True)):
    print("Cleaned investor loan details data")
else:
    print("No Data present to delete")

# COMMAND ----------

if(dbutils.fs.rm(f"{cleanedFiles_file_path}customer_details", True)):
        print("Cleaned customer details data")
else:
    print("No Data present to delete")

# COMMAND ----------

if(dbutils.fs.rm(f"{cleanedFiles_file_path}account_details", True)):
    print("Cleaned account details data")
else:
    print("No Data present to delete")

# COMMAND ----------

dbutils.notebook.exit("executed delete existing data job")
