# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: typed, standardized, deduplicated
# MAGIC Uses the tested functions in `src/creditlens/transforms.py`. Every bronze row ends up in exactly one place:
# MAGIC the clean table or the `rejects` table (with a reason). The notebook proves it with a tie-out.

# COMMAND ----------
import os, sys
sys.path.append(os.path.abspath("../src"))
from creditlens import transforms as T

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "creditlens")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
t = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# COMMAND ----------
b_c, b_l, b_p = (spark.table(t(f"bronze_{n}")) for n in ["customers", "loans", "payments"])

s_c, r_c = T.clean_customers(b_c)
s_l, r_l = T.clean_loans(b_l, s_c)
s_p, r_p = T.clean_payments(b_p, s_l)

for name, b, s, r in [("customers", b_c, s_c, r_c), ("loans", b_l, s_l, r_l), ("payments", b_p, s_p, r_p)]:
    s.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(f"silver_{name}"))
    r.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(f"rejects_{name}"))

# COMMAND ----------
# Source-to-target tie-out: bronze == silver + rejects, for every table.
for name in ["customers", "loans", "payments"]:
    rec = T.reconcile(spark.table(t(f"bronze_{name}")).count(), spark.table(t(f"silver_{name}")).count(),
                      spark.table(t(f"rejects_{name}")).count())
    print(name, rec)
    assert rec["balanced"], f"Tie-out failed for {name}"

# COMMAND ----------
display(spark.sql(f"""
  SELECT 'loans' AS tbl, reject_reason, COUNT(*) AS rows FROM {t('rejects_loans')} GROUP BY reject_reason
  UNION ALL SELECT 'payments', reject_reason, COUNT(*) FROM {t('rejects_payments')} GROUP BY reject_reason
  ORDER BY tbl, rows DESC"""))
