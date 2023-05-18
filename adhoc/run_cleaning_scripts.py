# Databricks notebook source
# MAGIC %run "/Repos/Big_Data_Project/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run mount_adls_storage.py once to mount before running the cleaning scripts

# COMMAND ----------

delete_clean_data=dbutils.notebook.run("/Repos/Big_Data_Project/Lending_Club/adhoc/delete_data",0)

# COMMAND ----------

if (delete_clean_data == "executed delete existing data job"):
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

status_account= dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Account",0)

# COMMAND ----------

if (status_account == "executed account job"):
    print("Lending loan account job completed successfully")

# COMMAND ----------

status_loan_defaulters=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_LoanDefaulters",0)

# COMMAND ----------

if (status_loan_defaulters == "executed loan defaulters job"):
    print("Lending loan defaulters job completed successfully")

# COMMAND ----------

status_loan_Payments=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Payments",0)

# COMMAND ----------

if (status_loan_Payments == "executed payments job"):
    print("Lending loan payments job completed successfully")

# COMMAND ----------

status_loan_Investor=dbutils.notebook.run(f"{cleanedScript_folder_path}DataCleaning_Investors",0)

# COMMAND ----------

if (status_loan_Investor == "executed investors job"):
    print("Lending loan investors job completed successfully")

# COMMAND ----------

# dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
