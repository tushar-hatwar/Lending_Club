# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Cleaning Process - Account Data

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

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

account_schema = StructType(fields=[StructField("acc_id", StringType(), False),
                                     StructField("mem_id", StringType(), False),
                                     StructField("loan_id", StringType(), False),
                                     StructField("grade", StringType(), True),
                                     StructField("sub_grade",StringType(), True),
                                     StructField("emp_title",StringType(), True),
                                     StructField("emp_length",StringType(), True),
                                     StructField("home_ownership",StringType(), True),
                                     StructField("annual_inc",FloatType(), True),
                                     StructField("verification_status",StringType(), True),
                                     StructField("tot_hi_cred_lim",FloatType(), True),
                                     StructField("application_type",StringType(), True),
                                     StructField("annual_inc_joint",FloatType(), True),
                                     StructField("verification_status_joint",StringType(), True)
                                    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the csv file into a dataframe

# COMMAND ----------

account_df=spark.read \
    .option("header",True) \
    .schema(account_schema) \
    .csv(f"{rawFiles_file_path}account_details.csv")

# COMMAND ----------

display(account_df)

# COMMAND ----------

account_df.createOrReplaceTempView("temp")
unique_values=spark.sql("select distinct emp_length from temp").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Optimized way to clean your dataframe having extra strings and symbols from a dataframe column

# COMMAND ----------

#clean_df = account_df.withColumn("emp_length", regexp_replace("emp_length", "[^0-9]", " "))
#int_df=clean_df.withColumn("emp_length", col("emp_length").cast("integer"))
#display(int_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another way to clean your dataframe having extra strings and symbols (Optional)

# COMMAND ----------

#above code is optimized to retain only the integers in the emp_length column
replace_value_na=account_df.withColumn("emp_length", when(col("emp_length")== lit("n/a"),lit("null")).otherwise(col("emp_length"))  )
display(replace_value_na)

# COMMAND ----------

replace_value_1yr=replace_value_na.withColumn("emp_length", when(col("emp_length")== lit("< 1 year"),lit("1")).otherwise(col("emp_length"))  )
display(replace_value_1yr)

# COMMAND ----------

replace_value_10yr=replace_value_1yr.withColumn("emp_length", when(col("emp_length")== lit("10+ years"),lit("10")).otherwise(col("emp_length"))  )
display(replace_value_10yr)

# COMMAND ----------

string_to_remove="years"
# Use the regexp_replace function to remove the string from the column
replace_value_years = replace_value_10yr.withColumn("emp_length", regexp_replace(replace_value_10yr["emp_length"], string_to_remove, ""))

# Display the resulting dataframe
display(replace_value_years)

# COMMAND ----------

string_to_remove="year"
# Use the regexp_replace function to remove the string from the column
clean_df= replace_value_years.withColumn("emp_length", regexp_replace(replace_value_years["emp_length"], string_to_remove, ""))

# Display the resulting dataframe
display(clean_df)

# COMMAND ----------

clean_df.createOrReplaceTempView("temp")
display_df=spark.sql("select distinct emp_length from temp ")
display(display_df)

# COMMAND ----------

clean_df.createOrReplaceTempView("temp")
display_df=spark.sql("select * from temp where emp_length='null' ")
display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace the NULL strings into NULL values

# COMMAND ----------

final_clean_df=clean_df.replace("null",None)
display(final_clean_df)


# COMMAND ----------

final_clean_df.createOrReplaceTempView("temp")
display_df=spark.sql("select * from temp where tot_hi_cred_lim is null ")
display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the ingestion date to the dataframe

# COMMAND ----------

account_df_ingestDate=ingestDate(final_clean_df)
display(account_df_ingestDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a surrogate key to the dataframe

# COMMAND ----------

account_df_key=account_df_ingestDate.withColumn("account_key", sha2(concat(col("acc_id"),col("mem_id"),col("loan_id")), 256))
display(account_df_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns in the dataframe

# COMMAND ----------

account_df_rename=account_df_key.withColumnRenamed("acc_id","account_id") \
.withColumnRenamed("mem_id","member_id") \
.withColumnRenamed("emp_title","employee_designation") \
.withColumnRenamed("emp_length","employee_experience") \
.withColumnRenamed("annual_inc","annual_income") \
.withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint","annual_income_joint") \



# COMMAND ----------

account_df_rename.createOrReplaceTempView("temp")
df=spark.sql("select * from temp where date(ingest_date)='2023-01-09'")
display(df)

# COMMAND ----------

account_df_rename.createOrReplaceTempView("temp_table")
final_df=spark.sql("select account_key,ingest_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_experience,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint from temp_table ")
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the cleaned dataframe into data lake

# COMMAND ----------

final_df.write.options(header='True').mode("append").parquet(f"{cleanedFiles_file_path}account_details")

# COMMAND ----------

display(spark.read.parquet(f"{cleanedFiles_file_path}account_details"))

# COMMAND ----------

dbutils.notebook.exit("executed account job")
