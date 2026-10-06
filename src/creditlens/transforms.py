"""Pure PySpark transformations for the CreditLens medallion pipeline.

Design rules (these are interview talking points):
* Every function takes DataFrames in and returns DataFrames out - no reads/writes,
  no globals - so each one can be unit-tested on a tiny local Spark session.
* Silver cleaning never silently drops rows: every raw row ends up either in the
  clean table or in a rejects table with a `reject_reason`. That gives an exact
  source-to-target tie-out: bronze_rows == silver_rows + reject_rows.
* Casting is ANSI-safe (regex-validate, then cast), so it behaves the same on
  Databricks serverless (ANSI on) and older Spark.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

VALID_STATUSES = ["Current", "Fully Paid", "Late", "Charged Off"]
_STATUS_MAP = {"current": "Current", "fully paid": "Fully Paid", "late": "Late", "charged off": "Charged Off"}


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def to_double(col: Column) -> Column:
    """'$12,345.50' / '13.5%' / ' 7 ' -> double; anything non-numeric -> null."""
    s = F.regexp_replace(F.trim(col), r"[$,%\s]", "")
    return F.when(s.rlike(r"^-?\d+(\.\d+)?$"), s.cast("double"))


def to_long(col: Column) -> Column:
    s = F.trim(col)
    return F.when(s.rlike(r"^\d+$"), s.cast("long"))


def to_date_multi(col: Column) -> Column:
    """Accept 2026-09-30 and 30/09/2026; invalid or other formats -> null."""
    s = F.trim(col)
    iso = F.try_to_timestamp(s, F.lit("yyyy-MM-dd")).cast("date")
    dmy = F.try_to_timestamp(s, F.lit("dd/MM/yyyy")).cast("date")
    return F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"), iso).when(s.rlike(r"^\d{2}/\d{2}/\d{4}$"), dmy)


def standardize_status(col: Column) -> Column:
    key = F.lower(F.trim(F.regexp_replace(col, r"[-_]", " ")))
    out = F.lit(None).cast("string")
    for k, v in reversed(list(_STATUS_MAP.items())):
        out = F.when(key == k, F.lit(v)).otherwise(out)
    return out


def _flag_duplicates(df: DataFrame, keys: list[str], order_col: str = "_ingest_ts") -> DataFrame:
    """Add `_rn`: 1 = the latest version of each key, >1 = older duplicates."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w))


def _split(df: DataFrame, reason: Column) -> tuple[DataFrame, DataFrame]:
    """Split into (clean, rejects). `reason` is null for good rows."""
    tagged = df.withColumn("reject_reason", reason)
    clean = tagged.where(F.col("reject_reason").isNull()).drop("reject_reason", "_rn")
    rejects = tagged.where(F.col("reject_reason").isNotNull()).drop("_rn")
    return clean, rejects


# --------------------------------------------------------------------------
# SILVER: bronze (all strings) -> typed, standardized, deduplicated
# --------------------------------------------------------------------------
def clean_customers(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = raw.select(
        to_long(F.col("customer_id")).alias("customer_id"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        to_double(F.col("annual_income")).alias("annual_income"),
        to_long(F.col("credit_score")).cast("int").alias("credit_score"),
        to_long(F.col("employment_years")).cast("int").alias("employment_years"),
        to_date_multi(F.col("signup_date")).alias("signup_date"),
        F.col("_ingest_ts"), F.col("_source_file"),
    )
    typed = _flag_duplicates(typed, ["customer_id"])
    reason = (
        F.when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("_rn") > 1, "duplicate")
        .when(~F.col("credit_score").between(300, 850) | F.col("credit_score").isNull(), "invalid_credit_score")
    )
    return _split(typed, reason)


def clean_loans(raw: DataFrame, customers_clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = raw.select(
        to_long(F.col("loan_id")).alias("loan_id"),
        to_long(F.col("customer_id")).alias("customer_id"),
        to_date_multi(F.col("origination_date")).alias("origination_date"),
        to_double(F.col("loan_amount")).alias("loan_amount"),
        to_long(F.col("term_months")).cast("int").alias("term_months"),
        to_double(F.col("interest_rate")).alias("interest_rate"),
        F.upper(F.trim(F.col("grade"))).alias("grade"),
        F.lower(F.trim(F.col("purpose"))).alias("purpose"),
        standardize_status(F.col("status")).alias("status"),
        F.col("_ingest_ts"), F.col("_source_file"),
    )
    known = customers_clean.select(F.col("customer_id").alias("_known_cid")).distinct()
    typed = typed.join(known, typed.customer_id == F.col("_known_cid"), "left")
    typed = _flag_duplicates(typed, ["loan_id"])
    reason = (
        F.when(F.col("loan_id").isNull(), "missing_loan_id")
        .when(F.col("_rn") > 1, "duplicate")
        .when(F.col("origination_date").isNull(), "invalid_origination_date")
        .when(F.col("loan_amount").isNull() | (F.col("loan_amount") <= 0), "invalid_loan_amount")
        .when(F.col("interest_rate").isNull() | ~F.col("interest_rate").between(0, 40), "invalid_interest_rate")
        .when(F.col("status").isNull(), "invalid_status")
        .when(F.col("_known_cid").isNull(), "orphan_customer")
    )
    clean, rejects = _split(typed, reason)
    return clean.drop("_known_cid"), rejects.drop("_known_cid")


def clean_payments(raw: DataFrame, loans_clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = raw.select(
        to_long(F.col("loan_id")).alias("loan_id"),
        to_date_multi(F.col("payment_date")).alias("payment_date"),
        to_double(F.col("amount_due")).alias("amount_due"),
        to_double(F.col("amount_paid")).alias("amount_paid"),
        to_long(F.col("days_past_due")).cast("int").alias("days_past_due"),
        F.col("_ingest_ts"), F.col("_source_file"),
    )
    known = loans_clean.select(F.col("loan_id").alias("_known_lid")).distinct()
    typed = typed.join(known, typed.loan_id == F.col("_known_lid"), "left")
    typed = _flag_duplicates(typed, ["loan_id", "payment_date"])
    reason = (
        F.when(F.col("loan_id").isNull() | F.col("payment_date").isNull(), "missing_key")
        .when(F.col("_rn") > 1, "duplicate")
        .when(F.col("amount_due").isNull(), "missing_amount_due")
        .when(F.col("days_past_due").isNull() | (F.col("days_past_due") < 0), "invalid_days_past_due")
        .when(F.col("_known_lid").isNull(), "orphan_loan")
    )
    clean, rejects = _split(typed, reason)
    return clean.drop("_known_lid"), rejects.drop("_known_lid")


def reconcile(bronze_rows: int, silver_rows: int, reject_rows: int) -> dict:
    """Source-to-target tie-out. Balanced means no row was lost or invented."""
    return {"bronze": bronze_rows, "silver": silver_rows, "rejects": reject_rows,
            "difference": bronze_rows - silver_rows - reject_rows,
            "balanced": bronze_rows == silver_rows + reject_rows}


# --------------------------------------------------------------------------
# GOLD: star schema + a risk mart
# --------------------------------------------------------------------------
def build_dim_customer(customers: DataFrame) -> DataFrame:
    score = F.col("credit_score")
    band = (F.when(score < 620, "Subprime").when(score < 660, "Near-prime")
            .when(score < 720, "Prime").otherwise("Super-prime"))
    inc = F.col("annual_income")
    income_band = (F.when(inc.isNull(), "Unknown").when(inc < 40_000, "Low")
                   .when(inc < 90_000, "Middle").otherwise("High"))
    return customers.select("customer_id", "state", "annual_income", "credit_score",
                            "employment_years", "signup_date",
                            band.alias("score_band"), income_band.alias("income_band"))


def build_dim_loan(loans: DataFrame) -> DataFrame:
    return loans.select(
        "loan_id", "customer_id", "origination_date",
        F.trunc("origination_date", "month").alias("origination_month"),
        F.add_months("origination_date", F.col("term_months")).alias("maturity_date"),
        "loan_amount", "term_months", "interest_rate", "grade", "purpose", "status",
        (F.col("status") == "Charged Off").alias("is_charged_off"),
    )


def build_fact_payments(payments: DataFrame) -> DataFrame:
    """Window functions: payment sequence, cumulative paid, running worst delinquency."""
    w_order = Window.partitionBy("loan_id").orderBy("payment_date")
    w_cum = w_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (payments
            .withColumn("payment_seq", F.row_number().over(w_order))
            .withColumn("cumulative_paid", F.sum("amount_paid").over(w_cum))
            .withColumn("max_dpd_to_date", F.max("days_past_due").over(w_cum))
            .withColumn("shortfall", F.round(F.col("amount_due") - F.col("amount_paid"), 2))
            .withColumn("is_30plus_dpd", F.col("days_past_due") >= 30)
            .select("loan_id", "payment_date", "payment_seq", "amount_due", "amount_paid",
                    "shortfall", "days_past_due", "is_30plus_dpd", "cumulative_paid", "max_dpd_to_date"))


def build_portfolio_risk_monthly(dim_loan: DataFrame, fact_payments: DataFrame) -> DataFrame:
    """One row per origination month x grade: the vintage view a risk team reads."""
    per_loan = (fact_payments.groupBy("loan_id")
                .agg(F.max("days_past_due").alias("worst_dpd"), F.sum("amount_paid").alias("total_paid")))
    j = dim_loan.join(per_loan, "loan_id", "left")
    return (j.groupBy("origination_month", "grade")
            .agg(F.count("*").alias("loans"),
                 F.round(F.sum("loan_amount"), 2).alias("originated_amount"),
                 F.sum(F.col("is_charged_off").cast("int")).alias("charged_off_loans"),
                 F.sum((F.coalesce(F.col("worst_dpd"), F.lit(0)) >= 30).cast("int")).alias("ever_30dpd_loans"),
                 F.round(F.avg("interest_rate"), 2).alias("avg_interest_rate"),
                 F.round(F.coalesce(F.sum("total_paid"), F.lit(0.0)), 2).alias("total_paid"))
            .withColumn("charge_off_rate", F.round(F.col("charged_off_loans") / F.col("loans"), 4))
            .withColumn("ever_30dpd_rate", F.round(F.col("ever_30dpd_loans") / F.col("loans"), 4))
            .orderBy("origination_month", "grade"))
