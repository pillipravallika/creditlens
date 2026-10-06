"""Run the whole pipeline on your laptop (no Databricks needed) and print a summary.

    python scripts/run_local.py --customers 20000 --loans 30000

Prints tie-outs, quality results and the risk mart. Add --write-gold to save Parquet files.
The Databricks notebooks do the same steps but write Delta tables in Unity Catalog.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from creditlens import datagen, quality as Q, transforms as T  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=20_000)
    
    ap.add_argument("--loans", type=int, default=30_000)
    ap.add_argument("--out", default="local_out")
    ap.add_argument("--write-gold", action="store_true",
                    help="also write gold tables as Parquet (on Windows this needs winutils; the notebooks write Delta instead)")
    a = ap.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("creditlens-local")
             .config("spark.sql.ansi.enabled", "true").config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw_dir = os.path.join(a.out, "raw")
    counts = datagen.write_csvs(datagen.generate(a.customers, a.loans), raw_dir)
    print("generated raw rows:", counts)

    # ---- bronze: land everything as strings + lineage columns -------------
    def bronze(name):
        return (spark.read.option("header", True).option("inferSchema", False).csv(os.path.join(raw_dir, f"{name}.csv"))
                .withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.lit(f"{name}.csv")))
    b_c, b_l, b_p = bronze("customers"), bronze("loans"), bronze("payments")

    # ---- silver ------------------------------------------------------------
    s_c, r_c = T.clean_customers(b_c)
    s_l, r_l = T.clean_loans(b_l, s_c)
    s_p, r_p = T.clean_payments(b_p, s_l)
    for name, b, s, r in [("customers", b_c, s_c, r_c), ("loans", b_l, s_l, r_l), ("payments", b_p, s_p, r_p)]:
        rec = T.reconcile(b.count(), s.count(), r.count())
        print(f"silver.{name}: {rec}")
        assert rec["balanced"], f"tie-out failed for {name}"
        r.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)

    # ---- gold ---------------------------------------------------------------
    dim_c, dim_l = T.build_dim_customer(s_c), T.build_dim_loan(s_l)
    fact_p = T.build_fact_payments(s_p)
    mart = T.build_portfolio_risk_monthly(dim_l, fact_p)
    if a.write_gold:
        for n, df in [("dim_customer", dim_c), ("dim_loan", dim_l), ("fact_payments", fact_p), ("portfolio_risk_monthly", mart)]:
            df.write.mode("overwrite").parquet(os.path.join(a.out, "gold", n))
    print("gold row counts:", {n: df.count() for n, df in [("dim_customer", dim_c), ("dim_loan", dim_l), ("fact_payments", fact_p), ("mart", mart)]})

    # ---- data quality on gold ----------------------------------------------
    results = [
        Q.not_null(dim_l, "dim_loan", "loan_id"), Q.unique(dim_l, "dim_loan", ["loan_id"]),
        Q.accepted_values(dim_l, "dim_loan", "status", T.VALID_STATUSES),
        Q.in_range(dim_l, "dim_loan", "interest_rate", 0, 40),
        Q.unique(dim_c, "dim_customer", ["customer_id"]),
        Q.relationships(dim_l, dim_c, "dim_loan", "customer_id", "customer_id"),
        Q.relationships(fact_p, dim_l, "fact_payments", "loan_id", "loan_id"),
    ]
    print("data quality:", Q.summarize(results))
    print("\nTop of the risk mart:")
    mart.orderBy(F.desc("charge_off_rate")).show(5)
    print("Overall charge-off rate by grade:")
    (dim_l.groupBy("grade").agg(F.count("*").alias("loans"), F.round(F.avg(F.col("is_charged_off").cast("int")), 3).alias("charge_off_rate"))
     .orderBy("grade").show())


if __name__ == "__main__":
    main()
