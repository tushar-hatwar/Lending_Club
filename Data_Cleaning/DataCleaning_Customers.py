# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Cleaning Process - Customers Data

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

from pyspark.sql.functions import  concat, current_timestamp,sha2,col

# COMMAND ----------

# MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

cleanedScript_folder_path

# COMMAND ----------

#Infer the schema of customer's data
customer_schema = StructType(fields=[StructField("cust_id", StringType(), True),
                                     StructField("mem_id", StringType(), True),
                                     StructField("fst_name", StringType(), False),
                                     StructField("lst_name", StringType(), False),
                                     StructField("prm_status", StringType(), False),
                                     StructField("age", IntegerType(), False),
                                     StructField("state", StringType(), False),
                                     StructField("country", StringType(), False)
                                    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file into a dataframe

# COMMAND ----------

#create a reader dataframe for customer's data
customer_df = spark.read \
.option("header", True) \
.schema(customer_schema) \
.csv(f"{rawFiles_file_path}loan_customer_data.csv")

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

customer_selected_df = customer_df.select(col("cust_id"), col("mem_id"), col("fst_name"), col("lst_name"), col("prm_status"), col("age"), col("state"), col("country"))


# COMMAND ----------

display(customer_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternate way to read the data files

# COMMAND ----------

#Optional way to read the csv files directly from dbfs
customer_df_dbfs=spark.read \
.option("header",True) \
.schema(customer_schema) \
.csv(f"{rawFiles_file_path}loan_customer_data.csv")

# COMMAND ----------

customer_selected_df = customer_df_dbfs.select(col("cust_id"), col("mem_id"), col("fst_name"), col("lst_name"), col("prm_status"), col("age"), col("state"), col("country")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns in the dataframe

# COMMAND ----------

#Rename the columns to a better understandable way
customer_df_change=customer_df.withColumnRenamed("cust_id","customer_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("fst_name","first_name") \
.withColumnRenamed("lst_name","last_name") \
.withColumnRenamed("prm_status","premium_status") \



# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the ingestion date to the dataframe

# COMMAND ----------

#Include a ingest date column to signify when it got ingested into our data lake
#customer_df_ingestDate=customer_df_change.withColumn("ingest_date", current_timestamp())

# COMMAND ----------

customer_df_ingestDate=ingestDate(customer_df_change)

# COMMAND ----------

display(customer_df_ingestDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a surrogate key to the dataframe

# COMMAND ----------

#Include a customer_key column which acts like a surrogate key in the table
#SHA-2 (Secure Hash Algorithm 2) is a set of cryptographic hash functions. It produces a 256-bit (32-byte) hash value and is generally considered to be a more secure.
customer_df_final=customer_df_ingestDate.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")), 256))
display(customer_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Spark SQL to query the data

# COMMAND ----------

#Using spark SQL to query the tables
customer_df_final.createOrReplaceTempView("temp_table")
display_df=spark.sql("select customer_key,ingest_date,customer_id,member_id,first_name,last_name,premium_status,age,state,country from temp_table")
display(display_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the cleaned dataframe into data lake

# COMMAND ----------

#write the final cleaned customers data to data lake
display_df.write.options(header='True').mode("append").parquet(f"{cleanedFiles_file_path}customer_details")

# COMMAND ----------

#List the files inside the customers folder
dbutils.fs.ls(f"{cleanedFiles_file_path}customer_details")

# COMMAND ----------

display(spark.read.parquet(f"{cleanedFiles_file_path}customer_details"))

# COMMAND ----------

dbutils.notebook.exit("executed customers job")
