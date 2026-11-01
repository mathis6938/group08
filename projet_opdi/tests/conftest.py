"""Shared fixtures: the package is imported from projet_opdi/src, Spark runs locally."""
import os
import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)          # Spark workers use the same Python

import opdi_pipeline  # noqa: E402,F401  (before any pyspark import, as in the pipeline)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("opdi-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
