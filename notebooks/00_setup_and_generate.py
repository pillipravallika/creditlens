# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup and synthetic data
# MAGIC Creates the schema and a Unity Catalog **Volume** (a managed file area), then writes the raw CSV files
# MAGIC (customers, loans, payments) into it. This plays the role of "source systems dropping files".
# MAGIC
# MAGIC All data is **synthetic**. No real customer or bank data is used.

# COMMAND ----------
import os, sys
sys.path.append(os.path.abspath("../src"))          # makes `import creditlens` work from the Git folder

dbutils.widgets.text("catalog", "workspace")        # Free Edition default catalog
dbutils.widgets.text("schema", "creditlens")
dbutils.widgets.text("n_customers", "20000")
dbutils.widgets.text("n_loans", "30000")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------
from creditlens import datagen

tables = datagen.generate(int(dbutils.widgets.get("n_customers")), int(dbutils.widgets.get("n_loans")))
print(datagen.write_csvs(tables, RAW))              # {'customers': ..., 'loans': ..., 'payments': ...}

# COMMAND ----------
display(dbutils.fs.ls(RAW))
