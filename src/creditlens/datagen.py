"""Synthetic retail-loan data generator.

Why synthetic? Real bank data is confidential, and public datasets need logins.
This generator is seeded (reproducible) and deliberately injects realistic mess
(duplicates, mixed date formats, "$" and "%" strings, bad casing, orphan keys)
so the silver layer has real cleaning work to do.

Nothing here is real customer data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

GRADES = ["A", "B", "C", "D", "E", "F", "G"]
BASE_PD = dict(zip(GRADES, [0.02, 0.05, 0.09, 0.14, 0.20, 0.27, 0.33]))
BASE_RATE = dict(zip(GRADES, [6.5, 9.0, 12.0, 15.0, 18.0, 21.0, 24.0]))
PURPOSES = ["debt_consolidation", "credit_card", "home_improvement", "car", "medical", "small_business"]
STATES = ["TX", "CA", "NY", "FL", "IL", "WA", "GA", "NC", "OH", "AZ"]


def _monthly_installment(amount: np.ndarray, annual_rate: np.ndarray, term: np.ndarray) -> np.ndarray:
    r = annual_rate / 100 / 12
    return amount * r / (1 - (1 + r) ** (-term))


def generate(n_customers: int = 20_000, n_loans: int = 30_000, as_of: str = "2026-09-30",
             seed: int = 42, dirty: bool = True) -> dict[str, pd.DataFrame]:
    """Return {'customers','loans','payments'} as all-string DataFrames (like raw CSV)."""
    rng = np.random.default_rng(seed)
    as_of_ts = pd.Timestamp(as_of)

    # ---- customers -------------------------------------------------------
    cust_id = np.arange(1, n_customers + 1)
    score = np.clip(rng.normal(690, 70, n_customers), 500, 840).round().astype(int)
    income = np.clip(rng.lognormal(11.0, 0.5, n_customers), 18_000, 400_000).round(-2)
    signup = as_of_ts - pd.to_timedelta(rng.integers(30, 3000, n_customers), unit="D")
    customers = pd.DataFrame({
        "customer_id": cust_id, "state": rng.choice(STATES, n_customers),
        "annual_income": income, "credit_score": score,
        "employment_years": rng.integers(0, 25, n_customers), "signup_date": signup.strftime("%Y-%m-%d"),
    })

    # ---- loans -----------------------------------------------------------
    loan_cust = rng.choice(cust_id, n_loans)
    c_score = customers.set_index("customer_id").loc[loan_cust, "credit_score"].to_numpy()
    # better score -> better grade (lower index)
    grade_idx = np.clip(((760 - c_score) / 40 + rng.normal(0, 0.8, n_loans)).round(), 0, 6).astype(int)
    grade = np.array(GRADES)[grade_idx]
    term = rng.choice([36, 60], n_loans, p=[0.7, 0.3])
    amount = np.clip(rng.lognormal(9.3, 0.6, n_loans), 1_000, 40_000).round(-2)
    rate = np.array([BASE_RATE[g] for g in grade]) + rng.normal(0, 0.8, n_loans)
    orig = as_of_ts - pd.to_timedelta(rng.integers(30, 1800, n_loans), unit="D")
    loan_id = np.arange(100_000, 100_000 + n_loans)

    months_elapsed = np.minimum(((as_of_ts - orig).days // 30).to_numpy(), term)
    pd_arr = np.clip(np.array([BASE_PD[g] for g in grade]) + (680 - c_score) / 1500, 0.005, 0.6)
    defaults = rng.random(n_loans) < pd_arr
    default_month = rng.integers(4, term + 1, n_loans)           # month the loan would default
    charged_off = defaults & (default_month <= months_elapsed)
    fully_paid = (~defaults) & (months_elapsed >= term)
    late_now = (~charged_off) & (~fully_paid) & (rng.random(n_loans) < 0.04)
    status = np.where(charged_off, "Charged Off", np.where(fully_paid, "Fully Paid",
                      np.where(late_now, "Late", "Current")))
    loans = pd.DataFrame({
        "loan_id": loan_id, "customer_id": loan_cust, "origination_date": orig.strftime("%Y-%m-%d"),
        "loan_amount": amount, "term_months": term, "interest_rate": rate.round(2),
        "grade": grade, "purpose": rng.choice(PURPOSES, n_loans), "status": status,
    })

    # ---- payments (one row per loan-month until payoff / charge-off) ------
    pay_months = np.where(charged_off, default_month - 1, months_elapsed)
    pay_months = np.maximum(pay_months, 0)
    idx = np.repeat(np.arange(n_loans), pay_months)
    seq = np.concatenate([np.arange(1, m + 1) for m in pay_months]) if len(idx) else np.array([], dtype=int)
    installment = _monthly_installment(amount, rate, term)[idx]
    pay_date = orig[idx] + pd.to_timedelta(seq * 30, unit="D")
    dpd = np.where(rng.random(len(idx)) < 0.07, rng.choice([15, 30, 60], len(idx)), 0)
    # the last months before a charge-off look worse
    near_default = charged_off[idx] & (seq >= pay_months[idx] - 2)
    dpd = np.where(near_default, np.maximum(dpd, 30), dpd)
    paid = np.where(dpd >= 30, installment * rng.choice([0.0, 0.5], len(idx)), installment)
    payments = pd.DataFrame({
        "loan_id": loan_id[idx], "payment_date": pay_date.strftime("%Y-%m-%d"),
        "amount_due": installment.round(2), "amount_paid": paid.round(2), "days_past_due": dpd,
    })

    out = {"customers": customers.astype(str), "loans": loans.astype(str), "payments": payments.astype(str)}
    if dirty:
        out = _inject_mess(out, rng)
    return out


def _inject_mess(t: dict[str, pd.DataFrame], rng: np.random.Generator) -> dict[str, pd.DataFrame]:
    c, l, p = t["customers"].copy(), t["loans"].copy(), t["payments"].copy()

    # customers: null incomes, "$12,345" formatting, duplicate rows
    m = rng.random(len(c)) < 0.03
    c.loc[m, "annual_income"] = ""
    m = rng.random(len(c)) < 0.15
    c.loc[m, "annual_income"] = c.loc[m, "annual_income"].map(lambda v: f"${float(v):,.0f}" if v else v)
    c = pd.concat([c, c.sample(frac=0.01, random_state=1)], ignore_index=True)

    # loans: mixed date formats, "13.5%" rates, status casing, negative amounts, orphans, duplicates
    m = rng.random(len(l)) < 0.20
    l.loc[m, "origination_date"] = pd.to_datetime(l.loc[m, "origination_date"]).dt.strftime("%d/%m/%Y")
    m = rng.random(len(l)) < 0.10
    l.loc[m, "interest_rate"] = l.loc[m, "interest_rate"] + "%"
    m = rng.random(len(l)) < 0.12
    l.loc[m, "status"] = l.loc[m, "status"].str.upper()
    m = rng.random(len(l)) < 0.05
    l.loc[m, "status"] = l.loc[m, "status"].str.replace(" ", "-", regex=False)
    m = rng.random(len(l)) < 0.002
    l.loc[m, "loan_amount"] = "-" + l.loc[m, "loan_amount"]
    m = rng.random(len(l)) < 0.003
    l.loc[m, "customer_id"] = "999999"                          # orphan: no such customer
    l = pd.concat([l, l.sample(frac=0.01, random_state=2)], ignore_index=True)

    # payments: duplicate rows, a few null due amounts
    p = pd.concat([p, p.sample(frac=0.005, random_state=3)], ignore_index=True)
    m = rng.random(len(p)) < 0.001
    p.loc[m, "amount_due"] = ""
    return {"customers": c, "loans": l, "payments": p}


def write_csvs(tables: dict[str, pd.DataFrame], out_dir: str) -> dict[str, int]:
    import os
    os.makedirs(out_dir, exist_ok=True)
    counts = {}
    for name, df in tables.items():
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)
        counts[name] = len(df)
    return counts
