# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: land the raw files as-is
# MAGIC **Rule of bronze:** keep the data exactly as received (all strings), and add lineage columns
# MAGIC (`_ingest_ts`, `_source_file`). Never fix data here - if the source was wrong, you want to be able to prove it.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "creditlens")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# COMMAND ----------
for name in ["customers", "loans", "payments"]:
    df = (spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW}/{name}.csv")
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{name}"))
    print(name, spark.table(f"{CATALOG}.{SCHEMA}.bronze_{name}").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### Try this (learning exercise)
# MAGIC Run `display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_loans"))` and look for: dates written two ways, `%` in rates,
# MAGIC `CHARGED-OFF` vs `Charged Off`, negative amounts. Write down every problem you can find *before* opening notebook 02.
