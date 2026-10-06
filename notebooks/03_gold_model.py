# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema and the risk mart
# MAGIC `dim_customer`, `dim_loan`, `fact_payments` (with window functions) and `portfolio_risk_monthly`
# MAGIC (charge-off and 30+ days-past-due rates by origination month and grade - a "vintage" view).

# COMMAND ----------
import os, sys
sys.path.append(os.path.abspath("../src"))
from creditlens import transforms as T

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "creditlens")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# COMMAND ----------
dim_c = T.build_dim_customer(spark.table(t("silver_customers")))
dim_l = T.build_dim_loan(spark.table(t("silver_loans")))
fact_p = T.build_fact_payments(spark.table(t("silver_payments")))
mart = T.build_portfolio_risk_monthly(dim_l, fact_p)

for name, df in [("dim_customer", dim_c), ("dim_loan", dim_l), ("fact_payments", fact_p), ("portfolio_risk_monthly", mart)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(f"gold_{name}"))
    print(name, spark.table(t(f"gold_{name}")).count())

# COMMAND ----------
# MAGIC %md ### Business question: which grades lose money?

# COMMAND ----------
display(spark.sql(f"""
  SELECT grade, SUM(loans) AS loans, SUM(charged_off_loans) AS charged_off,
         ROUND(SUM(charged_off_loans) / SUM(loans), 3) AS charge_off_rate,
         ROUND(SUM(originated_amount) / 1e6, 2) AS originated_musd
  FROM {t('gold_portfolio_risk_monthly')} GROUP BY grade ORDER BY grade"""))

# COMMAND ----------
# MAGIC %md ### Window-function practice: each loan's worst delinquency so far, plus its rank within its grade

# COMMAND ----------
display(spark.sql(f"""
  SELECT l.grade, l.loan_id, f.payment_seq, f.max_dpd_to_date, f.cumulative_paid,
         DENSE_RANK() OVER (PARTITION BY l.grade ORDER BY f.cumulative_paid DESC) AS paid_rank_in_grade
  FROM {t('gold_fact_payments')} f JOIN {t('gold_dim_loan')} l USING (loan_id)
  WHERE f.payment_seq = 12 ORDER BY l.grade, paid_rank_in_grade LIMIT 50"""))
