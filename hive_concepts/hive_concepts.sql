-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Hive Concepts
-- MAGIC
-- MAGIC 1. Understand the hive concepts in databricks
-- MAGIC 2. Database objects
-- MAGIC 3. Hive managed tables
-- MAGIC 4. Hive external tables
-- MAGIC 5. Create views

-- COMMAND ----------

-- MAGIC %run "/Lending_Club/adhoc/Call_functions_variables"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_tmp;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE lending_loan_tmp; 

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED lending_loan_tmp; 

-- COMMAND ----------

--DROP DATABASE lending_loan_tmp;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_dev LOCATION '/mnt/financestoragebig2023/cleaned-data/'

-- COMMAND ----------

USE default;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

--using lending_loan_tmp in hive dbfs
USE lending_loan_tmp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Hive Managed Tables
-- MAGIC 1. Create hive managed table
-- MAGIC 2. Read data from managed tables
-- MAGIC 3. Drop managed tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # reading the files which are in parquest format and creating a dataframe out of it
-- MAGIC customer_df = spark.read.parquet(f"{cleanedFiles_file_path}customer_details")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # saving the dataframe as table in dbfs:user/hive/warehouse location -> as it is managed table
-- MAGIC customer_df.write.format("parquet").saveAsTable("lending_loan_tmp.customer_details")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/lending_loan_tmp.db/customer_details"))

-- COMMAND ----------

DESCRIBE extended lending_loan_tmp.customer_details;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

select count(*) from lending_loan_tmp.customer_details

-- COMMAND ----------

--creating managed TABLE lending_loan_tmp.customer_genz 
CREATE OR REPLACE TABLE lending_loan_tmp.customer_genz as
SELECT * FROM lending_loan_tmp.customer_details WHERE age between 18 and 25

-- COMMAND ----------

DESCRIBE extended lending_loan_tmp.customer_genz;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from lending_loan_tmp.customer_genz;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

DROP TABLE lending_loan_tmp.customer_genz;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Hive External Tables
-- MAGIC 1. Create external hive tables
-- MAGIC 2. View the externally stored data
-- MAGIC 3. Drop external tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_dev LOCATION 'f{cleanedScript_folder_path}'

-- COMMAND ----------

USE lending_loan_dev;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customer_df_external = spark.read.parquet(f"{cleanedFiles_file_path}customer_details")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # A managed table will be created but the files (data) will be stored at external localtion 
-- MAGIC # in our case it will be stored in -> /mnt/financestoragebig2023/cleaned-data
-- MAGIC # Note this is not external table
-- MAGIC customer_df_external.write.format("parquet").saveAsTable("lending_loan_dev.customer_details_temp")

-- COMMAND ----------

DESC DATABASE EXTENDED lending_loan_dev;

-- COMMAND ----------

DESC EXTENDED lending_loan_dev.customer_details_temp;

-- COMMAND ----------

SELECT * FROM lending_loan_dev.customer_details_temp limit 100;

-- COMMAND ----------

--DROP TABLE lending_loan_tmp.customer_details_temp;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_tmp.customer_details_external
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
LOCATION "/mnt/financestoragebig2023/cleaned-data/customer_details_external"

-- COMMAND ----------

INSERT INTO lending_loan_tmp.customer_details_external
select * from lending_loan_tmp.customer_details

-- COMMAND ----------

select * from lending_loan_tmp.customer_details_external limit 100;

-- COMMAND ----------

--DROP TABLE lending_loan_tmp.customer_details_external
--Dropping External table won't delete the data from the location 

-- COMMAND ----------

DESC EXTENDED lending_loan_tmp.customer_details_external

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_tmp.customer_details_alaska 
USING PARQUET
LOCATION "/mnt/datasetbigdata/cleaned-data/customer_details_alaska"
AS
SELECT *
  FROM lending_loan_tmp.customer_details_external
 WHERE state='Alaska'

-- COMMAND ----------

-- DBTITLE 1,Check details of Alaska Table
DESC EXTENDED lending_loan_tmp.customer_details_alaska;

-- COMMAND ----------

SELECT * FROM  lending_loan_tmp.customer_details_alaska;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Different ways to create tables
-- MAGIC 1. Schema & Data : Databricks (managed hive tables)
-- MAGIC 2. Schema & Data : External (external hive tables)
-- MAGIC 3. Schema: Databricks Table: External (external hive tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

USE lending_loan_tmp;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_premium_customers_temp
AS
SELECT *
  FROM lending_loan_tmp.customer_details
 WHERE premium_status='TRUE'

-- COMMAND ----------

SELECT * FROM vw_premium_customers_temp;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW vw_state_customers_global
AS
SELECT * 
  FROM lending_loan_tmp.customer_details
 WHERE state like 'A%'

-- COMMAND ----------

SELECT distinct(state) FROM global_temp.vw_state_customers_global;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW vw_state_customers_permanent
AS
SELECT * 
  FROM lending_loan_tmp.customer_details
 WHERE state like 'N%'

-- COMMAND ----------

SELECT * FROM vw_state_customers_permanent;

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

SHOW VIEWS IN lending_loan_tmp;
