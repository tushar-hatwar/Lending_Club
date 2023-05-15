# Databricks notebook source
#calculate score for customers payment history

# COMMAND ----------

spark.conf.set("spark.sql.unacceptable_rated_pts",0)
spark.conf.set("spark.sql.very_bad_rated_pts",100)
spark.conf.set("spark.sql.bad_rated_pts",250)
spark.conf.set("spark.sql.good_rated_pts",500)
spark.conf.set("spark.sql.very_good_rated_pts",650)
spark.conf.set("spark.sql.excellent_rated_pts",800)

# COMMAND ----------

unacceptable_grade="F"
very_bad_grade="E"
bad_grade="D"
good_grade="C"
very_good_grade="B"
excellent_grade="A"

# COMMAND ----------

# Run call function varibles for getting all the paths 
%run "/Lending_Club/adhoc/Call_functions_variables"

# COMMAND ----------

account_df=spark.read.parquet(f"{cleanedFiles_file_path}account_details")

# COMMAND ----------

loan_df=spark.read.parquet(f"{cleanedFiles_file_path}loan_details")

# COMMAND ----------

payment_df=spark.read.parquet(f"{cleanedFiles_file_path}payment_details")

# COMMAND ----------

loan_def_df=spark.read.parquet(f"{cleanedFiles_file_path}loan_defaulters")

# COMMAND ----------

customer_df=spark.read.parquet(f"{cleanedFiles_file_path}customer_details")

# COMMAND ----------

customer_df.createOrReplaceTempView("customer_details")

# COMMAND ----------

payment_df.createOrReplaceTempView("payment_details")

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

spark.sql("SELECT member_id, state, country, first_name, last_name FROM customer_details ").show()

# COMMAND ----------

payment_df.printSchema()

# COMMAND ----------

spark.sql("SELECT loan_id, total_payment_recorded,installment, last_payment_amount,funded_amount_investor FROM payment_details LIMIT 100").show()

# COMMAND ----------

payment_last_df = spark.sql("select c.member_id, c.state, c.country, c.first_name, c.last_name, \
  case \
    when p.last_payment_amount < (p.installment * 0.5) then ${spark.sql.very_bad_rated_pts} \
    when p.last_payment_amount >= (p.installment * 0.5) and p.last_payment_amount < p.installment then ${spark.sql.bad_rated_pts} \
    when (p.last_payment_amount = (p.installment)) then ${spark.sql.good_rated_pts} \
    when p.last_payment_amount > (p.installment) and p.last_payment_amount <= (p.installment * 1.50) then ${spark.sql.very_good_rated_pts} \
    when p.last_payment_amount > (p.installment * 1.50) then ${spark.sql.excellent_rated_pts} \
    else ${spark.sql.unacceptable_rated_pts} \
  end as last_payment_pts, \
  case \
    when p.total_payment_recorded >= (p.funded_amount_investor * 0.50) then ${spark.sql.very_good_rated_pts} \
    when p.total_payment_recorded < (p.funded_amount_investor * 0.50) and p.total_payment_recorded > 0 then ${spark.sql.good_rated_pts} \
    when p.total_payment_recorded = 0 or (p.total_payment_recorded) is null then ${spark.sql.unacceptable_rated_pts} \
    end as total_payment_pts \
from payment_details p \
inner join customer_details c on c.member_id = p.member_id")


# COMMAND ----------

payment_last_df.createOrReplaceTempView("payment_points_df")

# COMMAND ----------

spark.sql("select * from payment_points_df where last_payment_pts!= 500 or total_payment_pts!=500 ").show()

# COMMAND ----------

loan_def_df.createOrReplaceTempView("loan_default_details")

# COMMAND ----------

loan_default_pts = spark.sql(
    "SELECT p.*, \
    CASE \
    WHEN l.defaulters_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.defaulters_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.defaulters_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.defaulters_2yrs > 5 OR l.defaulters_2yrs IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS delinq_pts, \
    CASE \
    WHEN l.public_records = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.public_records BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.public_records BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.public_records > 5 OR l.public_records IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END AS public_records_pts, \
    CASE \
    WHEN l.public_records_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts}  \
    WHEN l.public_records_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.public_records_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.public_records_bankruptcies > 5 OR l.public_records_bankruptcies IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS public_bankruptcies_pts, \
    CASE \
    WHEN l.enquiries_6mnths = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.enquiries_6mnths BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.enquiries_6mnths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.enquiries_6mnths > 5 OR l.enquiries_6mnths IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS enq_pts, \
    CASE \
    WHEN l.hardship_flag = 'N' THEN ${spark.sql.very_good_rated_pts} \
    WHEN l.hardship_flag = 'Y' OR l.hardship_flag IS NULL THEN ${spark.sql.bad_rated_pts} \
    END AS hardship_pts \
    FROM loan_default_details l \
    LEFT JOIN payment_points_df p ON p.member_id = l.member_id"
)


# COMMAND ----------

loan_default_pts.createOrReplaceTempView("loan_default_points_df")

# COMMAND ----------

loan_df.createOrReplaceTempView("loan_details")

# COMMAND ----------

account_df.createOrReplaceTempView("account_details")

# COMMAND ----------

# --200000 (20000) ==> 10%

# COMMAND ----------

financial_df = spark.sql("SELECT ldef.*, \
    CASE \
        WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${spark.sql.excellent_rated_pts} \
        WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${spark.sql.good_rated_pts} \
        WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${spark.sql.bad_rated_pts} \
        WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.very_bad_rated_pts} \
        WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${spark.sql.unacceptable_rated_pts} \
    END AS loan_status_pts, \
    CASE \
        WHEN LOWER(a.home_ownership) LIKE '%own%' THEN ${spark.sql.excellent_rated_pts} \
        WHEN LOWER(a.home_ownership) LIKE '%rent%' THEN ${spark.sql.good_rated_pts} \
        WHEN LOWER(a.home_ownership) LIKE '%mortgage%' THEN ${spark.sql.bad_rated_pts} \
        WHEN LOWER(a.home_ownership) LIKE '%any%' OR LOWER(a.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END AS home_pts,  \
    CASE \
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts}  \
        WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20)  THEN ${spark.sql.very_good_rated_pts}  \
        WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30)  THEN ${spark.sql.good_rated_pts}  \
        WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50)  THEN ${spark.sql.bad_rated_pts}  \
        WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70)  THEN ${spark.sql.very_bad_rated_pts}  \
        WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts}  \
    END AS credit_limit_pts, \
    CASE \
        WHEN (a.grade) ='A' and (a.sub_grade)='A1' THEN ${spark.sql.excellent_rated_pts}  \
        WHEN (a.grade) ='A' and (a.sub_grade)='A2' THEN (${spark.sql.excellent_rated_pts}* 0.80)  \
        WHEN (a.grade) ='A' and (a.sub_grade)='A3' THEN (${spark.sql.excellent_rated_pts}* 0.60)  \
        WHEN (a.grade) ='A' and (a.sub_grade)='A4' THEN (${spark.sql.excellent_rated_pts}* 0.40)  \
        WHEN (a.grade) ='A' and (a.sub_grade)='A5' THEN (${spark.sql.excellent_rated_pts}* 0.20)  \
        WHEN (a.grade) ='B' and (a.sub_grade)='B1' THEN (${spark.sql.very_good_rated_pts})  \
        WHEN (a.grade) ='B' and (a.sub_grade)='B2' THEN (${spark.sql.very_good_rated_pts}* 0.80)  \
        WHEN (a.grade) ='B' and (a.sub_grade)='B3' THEN (${spark.sql.very_good_rated_pts}* 0.60)  \
        WHEN (a.grade) ='B' and (a.sub_grade)='B4' THEN (${spark.sql.very_good_rated_pts}* 0.40)  \
        WHEN (a.grade) ='B' and (a.sub_grade)='B5' THEN (${spark.sql.very_good_rated_pts}* 0.20)  \
        WHEN (a.grade) ='C' and (a.sub_grade)='C1' THEN (${spark.sql.good_rated_pts})  \
        WHEN (a.grade) ='C' and (a.sub_grade)='C2' THEN (${spark.sql.good_rated_pts}* 0.80)  \
        WHEN (a.grade) ='C' and (a.sub_grade)='C3' THEN (${spark.sql.good_rated_pts}* 0.60)  \
        WHEN (a.grade) ='C' and (a.sub_grade)='C4' THEN (${spark.sql.good_rated_pts}* 0.40)  \
        WHEN (a.grade) ='C' and (a.sub_grade)='C5' THEN (${spark.sql.good_rated_pts}* 0.20)  \
        WHEN (a.grade) ='D' and (a.sub_grade)='D1' THEN (${spark.sql.bad_rated_pts})  \
        WHEN (a.grade) ='D' and (a.sub_grade)='D2' THEN (${spark.sql.bad_rated_pts}*0.80)  \
        WHEN (a.grade) ='D' and (a.sub_grade)='D3' THEN (${spark.sql.bad_rated_pts}*0.60)  \
        WHEN (a.grade) ='D' and (a.sub_grade)='D4' THEN (${spark.sql.bad_rated_pts}*0.40)  \
        WHEN (a.grade) ='D' and (a.sub_grade)='D5' THEN (${spark.sql.bad_rated_pts}*0.20)  \
        WHEN (a.grade) ='E' and (a.sub_grade)='E1' THEN (${spark.sql.very_bad_rated_pts})  \
        WHEN (a.grade) ='E' and (a.sub_grade)='E2' THEN (${spark.sql.very_bad_rated_pts}*0.80)  \
        WHEN (a.grade) ='E' and (a.sub_grade)='E3' THEN (${spark.sql.very_bad_rated_pts}*0.60)  \
        WHEN (a.grade) ='E' and (a.sub_grade)='E4' THEN (${spark.sql.very_bad_rated_pts}*0.40)  \
        WHEN (a.grade) ='E' and (a.sub_grade)='E5' THEN (${spark.sql.very_bad_rated_pts}*0.20)  \
        WHEN (a.grade) in ('F','G') and (a.sub_grade) in ('F1','G1') THEN (${spark.sql.unacceptable_rated_pts})  \
        WHEN (a.grade) in ('F','G') and (a.sub_grade) in ('F2','G2') THEN (${spark.sql.unacceptable_rated_pts}*0.80)  \
        WHEN (a.grade) in ('F','G') and (a.sub_grade) in ('F3','G3') THEN (${spark.sql.unacceptable_rated_pts}*0.60)  \
        WHEN (a.grade) in ('F','G') and (a.sub_grade) in ('F4','G4') THEN (${spark.sql.unacceptable_rated_pts}*0.40)  \
        WHEN (a.grade) in ('F','G') and (a.sub_grade) in ('F5','G5') THEN (${spark.sql.unacceptable_rated_pts}*0.20)  \
    END AS grade_pts \
 FROM loan_default_points_df ldef \
 LEFT JOIN loan_details l ON ldef.member_id = l.member_id \
 LEFT JOIN account_details a ON a.member_id = ldef.member_id")



# COMMAND ----------

financial_df.createOrReplaceTempView("loan_score_details")

# COMMAND ----------

financial_df.printSchema()

# COMMAND ----------

loan_score = spark.sql("SELECT member_id, first_name, last_name, state, country, \
((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, \
((delinq_pts +public_records_pts+public_bankruptcies_pts+enq_pts+hardship_pts)*0.45) as defaulters_history_pts, \
((loan_status_pts+home_pts+credit_limit_pts+grade_pts)*0.35) as financial_health_pts \
FROM loan_score_details")



# COMMAND ----------

loan_score.createOrReplaceTempView("loan_score_pts")

# COMMAND ----------

loan_score_final=spark.sql("select ls.member_id,ls.first_name,ls.last_name,ls.state,ls.country, \
(payment_history_pts+defaulters_history_pts+financial_health_pts) as loan_score \
from loan_score_pts ls ")

# COMMAND ----------

loan_score_final.createOrReplaceTempView("loan_score_eval")

# COMMAND ----------

loan_score_final=spark.sql("select ls.*, \
case \
WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN '" + excellent_grade + "' \
WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN '" + very_good_grade + "' \
WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN '" + good_grade + "' \
WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score > ${spark.sql.very_bad_grade_pts} THEN '" + bad_grade + "' \
WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN '" + very_bad_grade + "' \
WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN '" + unacceptable_grade + "' \
end as loan_final_grade \
from loan_score_eval ls")

# COMMAND ----------

loan_score_final.createOrReplaceTempView("loan_final_table")

# COMMAND ----------

spark.sql("select * from loan_final_table where loan_final_grade in ('B') ").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lending_loan_e2e.customers_loan_score
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/financestoragebig2023/processed-data/lending-loan/customer-transformations/customers_loan_score'
# MAGIC select * from loan_final_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM lending_loan_e2e.customers_loan_score limit 1000;

# COMMAND ----------


