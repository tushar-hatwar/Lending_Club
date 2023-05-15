# Databricks notebook source
# MAGIC %md
# MAGIC ### Pre-Prod Environment Vairables

# COMMAND ----------

rawFiles_file_path="/mnt/financestoragebig2023/raw-data/lending_loan/"

# COMMAND ----------

processed_file_path="/mnt/financestoragebig2023/processed-data/pre-prod/lending_loan/"

# COMMAND ----------

# cleanedScript_folder_path="/Lending_Club/Data_Cleaning/"  

# COMMAND ----------

cleanedFiles_file_path="/mnt/financestoragebig2023/cleaned-data/lending_loan/"

# COMMAND ----------

dbfs_file_path="/FileStore/tables/"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------


def ingestDate(input_df):
    date_df=input_df.withColumn("ingest_date",current_timestamp())
    return date_df

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/datasetbigdata/cleaned-data/lending_loan/account_details"))
