"""Shared fixtures.

The pipeline code lives in the project notebook. The unit tests do not copy it: they execute the notebook
cells that only define functions (cells tagged `pipeline-functions`, plus the imports and parameters cells)
and test those functions directly. A change in the notebook is therefore tested as it is.
"""
import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)          # Spark workers use the same Python

PROJECT_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK = PROJECT_DIR / "src" / "opdi_pipeline" / "project_pipeline_en_construction.ipynb"
STUBS = Path(__file__).resolve().parent / "stubs"
TAG = "pipeline-functions"

sys.path.insert(0, str(STUBS))          # findspark stand-in (PySpark comes from pip in the tests)
sys.path.insert(0, str(PROJECT_DIR))    # run_project.py


def notebook_cells():
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]


def code_of(cell):
    return "".join(cell["source"])


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


@pytest.fixture(scope="session")
def nb(spark):
    """Namespace with the notebook imports, parameters and every function cell executed."""
    cells = [c for c in notebook_cells() if c["cell_type"] == "code"]
    imports = next(c for c in cells if "findspark.init()" in code_of(c))
    params = next(c for c in cells if code_of(c).lstrip().startswith("# Default values = the project scope"))
    functions = [c for c in cells if TAG in c.get("metadata", {}).get("tags", [])]
    assert functions, f"no notebook cell tagged {TAG!r}"
    ns = {"spark": spark}
    for cell in [imports, params] + functions:
        exec(compile(code_of(cell), f"<notebook cell {cell['id']}>", "exec"), ns)
    return ns
