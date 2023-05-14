# Databricks notebook source
# MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

clean_data=dbutils.notebook.run("/Lending_Club/adhoc/delete_data",0)

# COMMAND ----------

if (clean_data == "executed delete existing data job"):
    print("delete existing data if any job completed successfully")

# COMMAND ----------

status_customers=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Customers",0)

# COMMAND ----------

if (status_customers == "executed customers job"):
    print("customers job completed successfully")

# COMMAND ----------

status_loan=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Loan",0)

# COMMAND ----------

if (status_loan == "executed loan job"):
    print("Lending loan job completed successfully")

# COMMAND ----------

status_loan=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Account",0)

# COMMAND ----------

if (status_loan == "executed account job"):
    print("Lending loan account job completed successfully")

# COMMAND ----------

status_loan=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_LoanDefaulters",0)

# COMMAND ----------

if (status_loan == "executed loan defaulters job"):
    print("Lending loan defaulters job completed successfully")

# COMMAND ----------

status_loan=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Payments",0)

# COMMAND ----------

if (status_loan == "executed payments job"):
    print("Lending loan payments job completed successfully")

# COMMAND ----------

status_loan=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Investors",0)

# COMMAND ----------

if (status_loan == "executed investors job"):
    print("Lending loan investors job completed successfully")
