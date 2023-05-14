# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Cleaning Process - Loan Defaulters Data

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

from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,when,sha2

# COMMAND ----------

# MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

loan_default_schema = StructType(fields=[StructField("loan_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("def_id", StringType(), False),
                                     StructField("delinq_2yrs", IntegerType(), True),
                                     StructField("delinq_amnt",FloatType(), True),
                                     StructField("pub_rec",IntegerType(), True),
                                     StructField("pub_rec_bankruptcies",IntegerType(), True),
                                     StructField("inq_last_6mths",IntegerType(), True),
                                     StructField("total_rec_late_fee",FloatType(), True),
                                     StructField("hardship_flag",StringType(), True),
                                     StructField("hardship_type",StringType(), True),
                                     StructField("hardship_length",IntegerType(), True),
                                     StructField("hardship_amount",FloatType(), True)

                                    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file into a dataframe

# COMMAND ----------

loan_default_df=spark.read.option("header",True).schema(loan_default_schema).csv(f"{rawFiles_file_path}loan_defaulters.csv")

# COMMAND ----------

display(loan_default_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the ingestion date to the dataframe

# COMMAND ----------

df_ingest_date=ingestDate(loan_default_df)
display(df_ingest_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a surrogate key to the dataframe

# COMMAND ----------

loan_default_key=df_ingest_date.withColumn("loan_default_key", sha2(concat(col("loan_id"),col("mem_id"),col("def_id")), 256))
display(loan_default_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace the NULL strings into NULL values

# COMMAND ----------

df_null=loan_default_key.replace("null",None)

# COMMAND ----------

df_null.createOrReplaceTempView("null_df_table")
display(spark.sql("select * from null_df_table where delinq_2yrs is null and hardship_flag is null"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns in the dataframe

# COMMAND ----------

loan_df_rename=loan_default_key.withColumnRenamed("mem_id", "member_id") \
.withColumnRenamed("def_id", "loan_default_id") \
.withColumnRenamed("delinq_2yrs", "defaulters_2yrs") \
.withColumnRenamed("delinq_amnt", "defaulters_amount") \
.withColumnRenamed("pub_rec", "public_records") \
.withColumnRenamed("pub_rec_bankruptcies", "public_records_bankruptcies") \
.withColumnRenamed("inq_last_6mths", "enquiries_6mnths") \
.withColumnRenamed("total_rec_late_fee", "late_fee") 


# COMMAND ----------

loan_df_rename.createOrReplaceTempView("temp")
display_df=spark.sql("select loan_default_key, ingest_date, loan_id,member_id,loan_default_id,defaulters_2yrs,defaulters_amount,public_records,public_records_bankruptcies,enquiries_6mnths,late_fee,hardship_flag,hardship_type,hardship_length,hardship_amount from temp")
display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the cleaned dataframe into data lake

# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleanedFiles_file_path}loan_defaulters")

# COMMAND ----------

display(spark.read.parquet(f"{cleanedFiles_file_path}loan_defaulters"))

# COMMAND ----------

dbutils.notebook.exit("executed loan defaulters job")
