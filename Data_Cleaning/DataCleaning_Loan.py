# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Cleaning Process - Loan Data

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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,DateType

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,sha2

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Project/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

loan_schema = StructType(fields=[StructField("loan_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("acc_id", StringType(), False),
                                     StructField("loan_amt", DoubleType(), True),
                                     StructField("fnd_amt", DoubleType(), True),
                                     StructField("term", StringType(), True),
                                     StructField("interest", StringType(), True),
                                     StructField("installment", FloatType(), True),
                                     StructField("issue_date", DateType(), True),
                                     StructField("loan_status", StringType(), True),
                                     StructField("purpose", StringType(), True),
                                     StructField("title", StringType(), True),
                                     StructField("disbursement_method", StringType(), True)
                                    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file into a dataframe

# COMMAND ----------

loan_df = spark.read \
.option("header", True) \
.schema(loan_schema) \
.csv(f"{rawFiles_file_path}loan_details.csv")

# COMMAND ----------

loan_df.createOrReplaceTempView("loan_table")
loan_sql=spark.sql("select * from loan_table")
display(loan_sql)

# COMMAND ----------

loan_sql.createOrReplaceTempView("loan_data")
loan_data_df=spark.sql("select * from loan_data where term=36 or interest > 5.0")
display(loan_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 03: Cleaning Techniques to include

# COMMAND ----------

# Define the string to remove
string_to_remove = "months"

# Use the regexp_replace function to remove the string from the column
clean_term_df = loan_df.withColumn("term", regexp_replace(loan_df["term"], string_to_remove, ""))

# Display the resulting dataframe
display(clean_term_df)

# COMMAND ----------

# Define the string to remove
string_to_remove = "%"

# Use the regexp_replace function to remove the string from the column
clean_interest_df = clean_term_df.withColumn("interest", regexp_replace(clean_term_df["interest"], string_to_remove, ""))

# Display the resulting dataframe
display(clean_interest_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns in the dataframe

# COMMAND ----------

loan_df_rename=clean_interest_df.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("acc_id","account_id") \
.withColumnRenamed("loan_amt","loan_amount") \
.withColumnRenamed("fnd_amt","funded_amount") \



# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the ingestion date to the dataframe

# COMMAND ----------

loan_df_ingestDate=ingestDate(loan_df_rename)
display(loan_df_ingestDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a surrogate key to the dataframe

# COMMAND ----------

loan_df_key=loan_df_ingestDate.withColumn("loan_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")), 256))
display(loan_df_key)

# COMMAND ----------

loan_df_key.createOrReplaceTempView("df_null")
null_df=spark.sql("select * from df_null where interest='null' ")
display(null_df)

# COMMAND ----------

loan_df_key.createOrReplaceTempView("df_null")
null_df=spark.sql("select * from df_null where interest is null ")
display(null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace the NULL strings into NULL values

# COMMAND ----------

final_df=loan_df_key.replace("null",None)

# COMMAND ----------

final_df.createOrReplaceTempView("df_null")
null_df=spark.sql("select * from df_null where interest is null ").show()

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.createOrReplaceTempView("loan_data")
loan_data_df=spark.sql("select * from loan_data where term=36 and interest > 5.0")
display(loan_data_df)

# COMMAND ----------

final_df.createOrReplaceTempView("temp_table")
display_df=spark.sql("select loan_key, ingest_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method from temp_table")
display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the cleaned dataframe into data lake

# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleanedFiles_file_path}loan_details")

# COMMAND ----------

dbutils.fs.ls(f"{cleanedFiles_file_path}loan_details")

# COMMAND ----------

display(spark.read.parquet(f"{cleanedFiles_file_path}loan_details"))

# COMMAND ----------

dbutils.notebook.exit("executed loan job")
