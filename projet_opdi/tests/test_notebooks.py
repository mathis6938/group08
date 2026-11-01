"""The showcase notebooks only call the package: their code must stay valid Python and use existing functions."""
import ast
import json
from pathlib import Path

import pytest

import opdi_pipeline.performance as performance

NOTEBOOKS = sorted((Path(__file__).resolve().parents[1] / "notebooks").glob("*.ipynb"))


def test_there_are_showcase_notebooks():
    assert [n.name for n in NOTEBOOKS] == ["01_data_understanding.ipynb", "02_pipeline_walkthrough.ipynb",
                                           "03_performance.ipynb", "04_gold_dashboard.ipynb"]


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_code_is_valid(notebook):
    cells = json.loads(notebook.read_text(encoding="utf-8"))["cells"]
    for cell in cells:
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            ast.parse(source)
    calls = {node.attr for cell in cells if cell["cell_type"] == "code"
             for node in ast.walk(ast.parse("".join(cell["source"])))
             if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "performance"}
    assert all(hasattr(performance, name) for name in calls), calls
