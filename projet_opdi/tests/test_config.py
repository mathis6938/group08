"""Settings of the environments (config/*.toml) and their overrides."""
import datetime

import pytest

from opdi_pipeline.config import load_settings


def test_dev_and_prod_are_separated():
    dev, prod = load_settings("dev"), load_settings("prod")
    assert (prod.start, prod.end) == (datetime.date(2026, 6, 1), datetime.date(2026, 7, 29))
    assert (dev.start, dev.end) == (datetime.date(2026, 6, 9), datetime.date(2026, 6, 19))
    assert dev.data_root != prod.data_root                         # a dev run never touches prod data
    assert prod.bronze_dir == prod.data_root / "bronze"
    assert dev.ref_urls["airports.csv"].endswith("airports.csv")
    assert dev.ml.test_days == 3 and prod.ml.test_days == 14


def test_overrides_precedence(monkeypatch, tmp_path):
    monkeypatch.setenv("OPDI_START", "2026-06-05")
    monkeypatch.setenv("OPDI_DATA_ROOT", str(tmp_path))
    s = load_settings("dev")
    assert s.start == datetime.date(2026, 6, 5) and s.data_root == tmp_path      # environment > file
    s = load_settings("dev", start="2026-06-07", end=None)
    assert s.start == datetime.date(2026, 6, 7) and s.end == datetime.date(2026, 6, 19)  # argument > environment


def test_full_refresh_switch(monkeypatch):
    assert load_settings("dev").full_refresh is False
    monkeypatch.setenv("OPDI_FULL_REFRESH", "true")                 # workflow_dispatch input in GitHub Actions
    assert load_settings("dev").full_refresh is True
    monkeypatch.setenv("OPDI_FULL_REFRESH", "false")
    assert load_settings("dev").full_refresh is False
    assert load_settings("dev", full_refresh=True).full_refresh is True       # --full-refresh


def test_invalid_settings():
    with pytest.raises(ValueError):
        load_settings("staging")
    with pytest.raises(ValueError):
        load_settings("dev", start="2026-06-19", end="2026-06-09")
