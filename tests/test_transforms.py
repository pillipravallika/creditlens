from datetime import date

from creditlens import transforms as T


def _cust(spark, rows):
    cols = ["customer_id", "state", "annual_income", "credit_score", "employment_years", "signup_date", "_ingest_ts"]
    df = spark.createDataFrame(rows, cols)
    from pyspark.sql import functions as F
    return df.withColumn("_ingest_ts", F.col("_ingest_ts").cast("timestamp")).withColumn("_source_file", F.lit("c.csv"))


def _loans(spark, rows):
    cols = ["loan_id", "customer_id", "origination_date", "loan_amount", "term_months",
            "interest_rate", "grade", "purpose", "status", "_ingest_ts"]
    from pyspark.sql import functions as F
    df = spark.createDataFrame(rows, cols)
    return df.withColumn("_ingest_ts", F.col("_ingest_ts").cast("timestamp")).withColumn("_source_file", F.lit("l.csv"))


def test_customers_clean_money_and_dedupe(spark):
    raw = _cust(spark, [
        ("1", " tx ", "$52,000", "700", "5", "2020-01-15", "2026-01-01 00:00:00"),
        ("1", "TX", "$53,000", "700", "5", "2020-01-15", "2026-02-01 00:00:00"),   # newer duplicate wins
        ("2", "CA", "", "650", "2", "15/03/2021", "2026-01-01 00:00:00"),           # null income, dd/MM/yyyy
        ("3", "NY", "80000", "1200", "9", "2019-05-05", "2026-01-01 00:00:00"),    # bad score
    ])
    clean, rejects = T.clean_customers(raw)
    got = {r.customer_id: r for r in clean.collect()}
    assert set(got) == {1, 2}
    assert got[1].annual_income == 53000.0 and got[1].state == "TX"
    assert got[2].annual_income is None and got[2].signup_date == date(2021, 3, 15)
    reasons = sorted(r.reject_reason for r in rejects.collect())
    assert reasons == ["duplicate", "invalid_credit_score"]
    # tie-out: nothing lost
    assert T.reconcile(raw.count(), clean.count(), rejects.count())["balanced"]


def test_loans_clean_rules(spark):
    customers, _ = T.clean_customers(_cust(spark, [("1", "TX", "50000", "700", "3", "2020-01-01", "2026-01-01 00:00:00")]))
    raw = _loans(spark, [
        ("10", "1", "2024-03-01", "$10,000", "36", "13.5%", "b", "Car", "CHARGED-OFF", "2026-01-01 00:00:00"),  # all dirty but valid
        ("11", "1", "2024-03-01", "-500", "36", "10", "B", "car", "Current", "2026-01-01 00:00:00"),            # negative amount
        ("12", "999", "2024-03-01", "5000", "36", "10", "B", "car", "Current", "2026-01-01 00:00:00"),           # orphan
        ("13", "1", "2024-03-01", "5000", "36", "10", "B", "car", "Weird", "2026-01-01 00:00:00"),               # bad status
        ("14", "1", "31/02/2024", "5000", "36", "10", "B", "car", "Late", "2026-01-01 00:00:00"),                # impossible date
        ("15", "1", "2024-03-01", "5000", "36", "99", "B", "car", "Late", "2026-01-01 00:00:00"),                # rate out of range
    ])
    clean, rejects = T.clean_loans(raw, customers)
    rows = clean.collect()
    assert [r.loan_id for r in rows] == [10]
    r = rows[0]
    assert (r.loan_amount, r.interest_rate, r.grade, r.purpose, r.status) == (10000.0, 13.5, "B", "car", "Charged Off")
    reasons = {x.loan_id: x.reject_reason for x in rejects.collect()}
    assert reasons == {11: "invalid_loan_amount", 12: "orphan_customer", 13: "invalid_status",
                       14: "invalid_origination_date", 15: "invalid_interest_rate"}
    assert T.reconcile(raw.count(), clean.count(), rejects.count())["balanced"]


def test_payments_dedupe_and_orphans(spark):
    from pyspark.sql import functions as F
    customers, _ = T.clean_customers(_cust(spark, [("1", "TX", "50000", "700", "3", "2020-01-01", "2026-01-01 00:00:00")]))
    loans, _ = T.clean_loans(_loans(spark, [("10", "1", "2024-03-01", "10000", "36", "10", "B", "car", "Current", "2026-01-01 00:00:00")]), customers)
    cols = ["loan_id", "payment_date", "amount_due", "amount_paid", "days_past_due", "_ingest_ts"]
    raw = (spark.createDataFrame([
        ("10", "2024-04-01", "300", "300", "0", "2026-01-01 00:00:00"),
        ("10", "2024-04-01", "300", "300", "0", "2026-01-02 00:00:00"),   # duplicate
        ("10", "2024-05-01", "", "300", "0", "2026-01-01 00:00:00"),      # missing due
        ("77", "2024-04-01", "300", "300", "0", "2026-01-01 00:00:00"),   # orphan
    ], cols).withColumn("_ingest_ts", F.col("_ingest_ts").cast("timestamp")).withColumn("_source_file", F.lit("p.csv")))
    clean, rejects = T.clean_payments(raw, loans)
    assert clean.count() == 1
    assert sorted(r.reject_reason for r in rejects.collect()) == ["duplicate", "missing_amount_due", "orphan_loan"]


def test_gold_fact_payments_window_columns(spark):
    pay = spark.createDataFrame(
        [(10, date(2024, 4, 1), 300.0, 300.0, 0), (10, date(2024, 5, 1), 300.0, 150.0, 30),
         (10, date(2024, 6, 1), 300.0, 300.0, 0)],
        "loan_id long, payment_date date, amount_due double, amount_paid double, days_past_due int")
    rows = T.build_fact_payments(pay).orderBy("payment_seq").collect()
    assert [r.payment_seq for r in rows] == [1, 2, 3]
    assert [r.cumulative_paid for r in rows] == [300.0, 450.0, 750.0]
    assert [r.max_dpd_to_date for r in rows] == [0, 30, 30]          # worst delinquency sticks
    assert [r.is_30plus_dpd for r in rows] == [False, True, False]
    assert rows[1].shortfall == 150.0


def test_gold_risk_mart_numbers(spark):
    loans = spark.createDataFrame([
        (1, 1, date(2024, 1, 15), 1000.0, 36, 10.0, "A", "car", "Current"),
        (2, 1, date(2024, 1, 20), 3000.0, 36, 12.0, "A", "car", "Charged Off"),
        (3, 1, date(2024, 2, 5), 2000.0, 36, 20.0, "C", "car", "Current"),
    ], "loan_id long, customer_id long, origination_date date, loan_amount double, term_months int, "
       "interest_rate double, grade string, purpose string, status string")
    pay = spark.createDataFrame([
        (1, date(2024, 2, 15), 40.0, 40.0, 0), (2, date(2024, 2, 20), 100.0, 0.0, 60), (3, date(2024, 3, 5), 70.0, 70.0, 0),
    ], "loan_id long, payment_date date, amount_due double, amount_paid double, days_past_due int")
    mart = T.build_portfolio_risk_monthly(T.build_dim_loan(loans), T.build_fact_payments(pay))
    jan_a = [r for r in mart.collect() if r.grade == "A"][0]
    assert (jan_a.loans, jan_a.originated_amount, jan_a.charged_off_loans) == (2, 4000.0, 1)
    assert jan_a.charge_off_rate == 0.5 and jan_a.ever_30dpd_rate == 0.5
    assert jan_a.avg_interest_rate == 11.0 and jan_a.total_paid == 40.0
    assert mart.count() == 2        # Jan/A and Feb/C


def test_dim_customer_bands(spark):
    df = spark.createDataFrame([(1, "TX", 30000.0, 600, 2, date(2020, 1, 1)), (2, "TX", None, 750, 5, date(2020, 1, 1))],
                               "customer_id long, state string, annual_income double, credit_score int, employment_years int, signup_date date")
    r = {x.customer_id: x for x in T.build_dim_customer(df).collect()}
    assert (r[1].score_band, r[1].income_band) == ("Subprime", "Low")
    assert (r[2].score_band, r[2].income_band) == ("Super-prime", "Unknown")
