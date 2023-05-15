-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creation of hive tables 

-- COMMAND ----------

-- MAGIC %run "/Repos/Big_Data_Project/Lending_Club/adhoc/Call_functions_variables"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_e2e;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Hive tables

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.customer_details_external
(
customer_key STRING,
ingest_date TIMESTAMP,
customer_id STRING,
member_id STRING,
first_name STRING,
last_name STRING,
premium_status STRING ,
age INT,
state STRING,
country STRING
)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/customer_details"

--Above table will read data from LOCATION specified

-- COMMAND ----------

DESCRIBE EXTENDED lending_loan_e2e.customer_details_external;

-- COMMAND ----------

select count(*) from lending_loan_e2e.customer_details_external;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.loan_details_external
(
loan_key STRING,
ingest_date TIMESTAMP,
loan_id STRING,
member_id STRING,
account_id STRING,
loan_amount DOUBLE,
funded_amount DOUBLE,
term INT,
interest FLOAT,
installment FLOAT,
issue_date DATE,
loan_status STRING,
purpose STRING,
title STRING,
disbursement_method STRING

)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/loan_details"

-- COMMAND ----------

SELECT count(*) FROM lending_loan_e2e.loan_details_external;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.account_details_external
(
account_key STRING,
ingest_date TIMESTAMP,
account_id STRING,
member_id STRING,
loan_id STRING,
grade STRING,
sub_grade STRING,
employee_designation STRING,
employee_experience INT,
home_ownership STRING,
annual_income FLOAT,
verification_status STRING,
total_high_credit_limit FLOAT,
application_type STRING,
annual_income_joint FLOAT,
verification_status_joint STRING

)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/loan_details"

-- COMMAND ----------

select count(*) from lending_loan_e2e.account_details_external;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.payment_details_external
(
payment_key STRING,
ingest_date TIMESTAMP,
loan_id STRING,
member_id STRING,
latest_transaction_id STRING,
funded_amount_investor DOUBLE,
total_payment_recorded FLOAT,
installment FLOAT,
last_payment_amount FLOAT,
last_payment_date DATE,
next_payment_date DATE,
payment_method STRING

)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/payment_details"

-- COMMAND ----------

SELECT count(*) FROM lending_loan_e2e.payment_details_external;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.loanDefaulters_details_external
(
loan_default_key STRING,
ingest_date TIMESTAMP,
loan_id STRING,
member_id STRING,
loan_default_id STRING,
defaulters_2yrs FLOAT,
defaulters_amount DOUBLE,
public_records INT,
public_records_bankruptcies INT,
enquiries_6mnths INT,
late_fee FLOAT,
hardship_flag STRING,
hardship_type STRING,
hardship_length INT,
hardship_amount FLOAT

)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/loan_defaulters"

-- COMMAND ----------

SELECT count(*) FROM lending_loan_e2e.loanDefaulters_details_external;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_e2e.investors_details_external
(
investor_loan_key STRING,
ingest_date TIMESTAMP,
investor_loan_id STRING,
loan_id STRING,
investor_id STRING,
loan_funded_amt FLOAT,
investor_type STRING,
age INT,
state STRING,
country STRING

)
USING PARQUET
LOCATION "/mnt/financestoragebig2023/cleaned-data/lending_loan/investor_loan_details"

-- COMMAND ----------

SELECT count(*) FROM lending_loan_e2e.investors_details_external;
