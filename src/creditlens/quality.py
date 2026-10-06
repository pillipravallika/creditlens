"""Tiny, dependency-free data-quality framework (same idea as Great Expectations / dbt tests).

Each check returns a dict so results can be stored as a Delta table and trended over time.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _result(table: str, check: str, column: str, failed: int, total: int) -> dict:
    return {"table": table, "check": check, "column": column,
            "failed_rows": failed, "total_rows": total, "passed": failed == 0}


def not_null(df: DataFrame, table: str, column: str) -> dict:
    return _result(table, "not_null", column, df.where(F.col(column).isNull()).count(), df.count())


def unique(df: DataFrame, table: str, columns: list[str]) -> dict:
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    return _result(table, "unique", ",".join(columns), total - distinct, total)


def accepted_values(df: DataFrame, table: str, column: str, values: list) -> dict:
    return _result(table, "accepted_values", column,
                   df.where(~F.col(column).isin(values) | F.col(column).isNull()).count(), df.count())


def in_range(df: DataFrame, table: str, column: str, low: float, high: float) -> dict:
    return _result(table, "in_range", column,
                   df.where(~F.col(column).between(low, high) | F.col(column).isNull()).count(), df.count())


def relationships(child: DataFrame, parent: DataFrame, table: str, child_key: str, parent_key: str) -> dict:
    """Referential integrity: every child key must exist in the parent."""
    orphans = child.join(parent.select(F.col(parent_key).alias("_pk")), child[child_key] == F.col("_pk"), "left_anti")
    return _result(table, "relationships", f"{child_key}->{parent_key}", orphans.count(), child.count())


def summarize(results: list[dict]) -> dict:
    failed = [r for r in results if not r["passed"]]
    return {"checks": len(results), "failed": len(failed), "failed_checks": failed}
