# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality gate
# MAGIC Runs checks on the gold tables, stores results in `dq_results` (so you can trend them), and **fails the
# MAGIC notebook** if any check fails. That failure is what makes a scheduled Job stop and alert instead of
# MAGIC publishing bad numbers.

# COMMAND ----------
import os, sys
sys.path.append(os.path.abspath("../src"))
from creditlens import quality as Q, transforms as T
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "creditlens")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
dim_l, dim_c, fact_p = (spark.table(t(f"gold_{n}")) for n in ["dim_loan", "dim_customer", "fact_payments"])

# COMMAND ----------
results = [
    Q.not_null(dim_l, "dim_loan", "loan_id"), Q.unique(dim_l, "dim_loan", ["loan_id"]),
    Q.accepted_values(dim_l, "dim_loan", "status", T.VALID_STATUSES),
    Q.in_range(dim_l, "dim_loan", "interest_rate", 0, 40),
    Q.in_range(dim_l, "dim_loan", "loan_amount", 1, 1_000_000),
    Q.unique(dim_c, "dim_customer", ["customer_id"]),
    Q.relationships(dim_l, dim_c, "dim_loan", "customer_id", "customer_id"),
    Q.relationships(fact_p, dim_l, "fact_payments", "loan_id", "loan_id"),
    Q.unique(fact_p, "fact_payments", ["loan_id", "payment_date"]),
]
out = (spark.createDataFrame(results).withColumn("run_ts", F.current_timestamp()))
out.write.format("delta").mode("append").saveAsTable(t("dq_results"))
display(out)

summary = Q.summarize(results)
print(summary["checks"], "checks,", summary["failed"], "failed")
assert summary["failed"] == 0, f"Data quality gate FAILED: {summary['failed_checks']}"
