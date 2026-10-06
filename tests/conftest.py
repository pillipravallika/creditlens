import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
# Make Spark workers use the same Python as this process (avoids "Python worker failed to connect" on Windows)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("creditlens-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ansi.enabled", "true")      # same setting as Databricks serverless
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()
