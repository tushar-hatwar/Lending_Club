# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Cleaning Process - Investors Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 01:
# MAGIC 1. Study and analyze the data
# MAGIC 2. Come up with scope for cleaning the data
# MAGIC 3. Bring out the cleaning techniques to be applied
# MAGIC 4. Include the new columns to be added

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 02:
# MAGIC 1. Infer the schema of the tables
# MAGIC 2. Read the data from the files
# MAGIC 3. Query the data
# MAGIC 4. Data cleaning techniques 
# MAGIC 5. New column additions
# MAGIC 6. Write the data to data lake

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp,sha2

# COMMAND ----------

# MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

#Infer the schema of customer's data
investor_schema = StructType(fields=[StructField("investor_loan_id", StringType(), True),
                                     StructField("loan_id", StringType(), True),
                                     StructField("investor_id", StringType(), False),
                                     StructField("loan_funded_amt", DoubleType(), False),
                                     StructField("investor_type", StringType(), False),
                                     StructField("age", IntegerType(), False),
                                     StructField("state", StringType(), False),
                                     StructField("country", StringType(), False)
                                    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file into a dataframe

# COMMAND ----------

#create a reader dataframe for investor's data
investor_df = spark.read \
.option("header", True) \
.schema(investor_schema) \
.csv(f"{rawFiles_file_path}loan_investors.csv")

# COMMAND ----------

investor_df.createOrReplaceTempView("investor_data")
spark.sql("select * from investor_data").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the ingestion date to the dataframe

# COMMAND ----------

#Include a ingest date column to signify when it got ingested into our data lake
investor_df_ingestDate=ingestDate(investor_df)
display(investor_df_ingestDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a surrogate key to the dataframe

# COMMAND ----------

#Include a customer_key column which acts like a surrogate key in the table
investor_df_key=investor_df_ingestDate.withColumn("investor_loan_key", sha2(concat(col("investor_loan_id"),col("loan_id"),col("investor_id")), 256))
display(investor_df_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Spark SQL to query the data

# COMMAND ----------

#Using spark SQL to query the tables
investor_df_key.createOrReplaceTempView("investor_temp_table")
display_df=spark.sql("select investor_loan_key,ingest_date,investor_loan_id,loan_id,investor_id,loan_funded_amt,investor_type,age,state,country from investor_temp_table")
display(display_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the cleaned dataframe into data lake

# COMMAND ----------

#write the final cleaned customers data to data lake
display_df.write.options(header='True').mode("append").parquet(f"{cleanedFiles_file_path}investor_loan_details")

# COMMAND ----------

#List the files inside the customers folder
display(dbutils.fs.ls(f"{cleanedFiles_file_path}investor_loan_details"))

# COMMAND ----------

display(spark.read.parquet(f"{cleanedFiles_file_path}investor_loan_details"))

# COMMAND ----------

dbutils.notebook.exit("executed investors job")
