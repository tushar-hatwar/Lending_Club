-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Query to find the total count of customers by state and country

-- COMMAND ----------

--Query to find the total count of customers by state and country from highest to lowest:
SELECT state, country, count(*) as total_count
FROM lending_loan_e2e.customer_details_external
GROUP BY state, country
order by total_count desc;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Query to find the count of customers having premium membership falling in different age buckets

-- COMMAND ----------

--Query to find the count of customers having premium membership falling in different age buckets
SELECT country, 
       CASE 
           WHEN age BETWEEN 18 AND 25 THEN 'Youngsters'
           WHEN age BETWEEN 26 AND 35 THEN 'Working class'
           WHEN age BETWEEN 36 AND 45 THEN 'Middle Age'
           ELSE 'Senior Citizens'
       END as age_range,
       COUNT(*) as premium_count
 FROM lending_loan_e2e.customer_details_external
WHERE premium_status = 'TRUE'
GROUP BY country, age_range;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Query to find the percentage of customers in each state that are premium customers, grouped by country

-- COMMAND ----------

--Query to find the percentage of customers in each state that are premium customers, grouped by country:
WITH customer_counts AS (
    SELECT country, state, COUNT(*) as total_customers
    FROM lending_loan_e2e.customer_details_external
    GROUP BY country, state
),
member_counts AS (
    SELECT country, state, COUNT(DISTINCT member_id) as total_members
    FROM lending_loan_e2e.customer_details_external
    WHERE member_id IS NOT NULL and  premium_status = 'TRUE'
    GROUP BY country, state
)
SELECT customer_counts.country, customer_counts.state, 
       ROUND(member_counts.total_members / customer_counts.total_customers * 100, 2) as member_percentage
FROM customer_counts
JOIN member_counts
ON customer_counts.country = member_counts.country AND customer_counts.state = member_counts.state;


-- COMMAND ----------

-- MAGIC %run "/Repos/Big_Data_Project/Lending_Club/adhoc/Call_functions_variables"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import avg, count,col,when,countDistinct,round
-- MAGIC customers_df=spark.read.parquet(f"{cleanedFiles_file_path}customer_details")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Query to find the number of customers with a premium status of "true" in each country, grouped by age range using pyspark dataframe:
-- MAGIC
-- MAGIC
-- MAGIC customers_df.filter("premium_status = 'TRUE'" ) \
-- MAGIC   .withColumn("age_range", when((col("age") >= 18) & (col("age") <= 25), "Youngsters")
-- MAGIC                            .when((col("age") > 25) & (col("age") <= 35), "Working class")
-- MAGIC                            .when((col("age") > 35) & (col("age") <= 45), "Middle Age")
-- MAGIC                            .otherwise("Senior citizens")) \
-- MAGIC   .groupBy("country", "age_range") \
-- MAGIC   .agg(count("*")) \
-- MAGIC   .show()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Query to find the average age of customers by state and country using pyspark dataframe
-- MAGIC customer_avg_age=customers_df.groupBy("state", "country").agg(avg("age"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC customer_avg_age.createOrReplaceTempView("customers_avg_age")

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.customers_avg_age 
USING PARQUET
LOCATION '/mnt/financestoragebig2023/processed-data/lending-loan/customer-transformations/customers_avg_age'
select * from customers_avg_age

-- COMMAND ----------

SELECT * FROM lending_loan_e2e.customers_avg_age

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.customers_premium_status 
USING PARQUET
LOCATION '/mnt/financestoragebig2023/processed-data/lending-loan/customer-transformations/customers_premium_status'

WITH customer_counts AS (
    SELECT country, state, COUNT(*) as total_customers
    FROM lending_loan_e2e.customer_details_external
    GROUP BY country, state
),
member_counts AS (
    SELECT country, state, COUNT(DISTINCT member_id) as total_members
    FROM lending_loan_e2e.customer_details_external
    WHERE member_id IS NOT NULL and  premium_status = 'TRUE'
    GROUP BY country, state
)
SELECT customer_counts.country, customer_counts.state, 
       ROUND(member_counts.total_members / customer_counts.total_customers * 100, 2) as member_percentage
FROM customer_counts
JOIN member_counts
ON customer_counts.country = member_counts.country AND customer_counts.state = member_counts.state;


-- COMMAND ----------

select * from lending_loan_e2e.customers_premium_status
