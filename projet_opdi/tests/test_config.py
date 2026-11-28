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


def test_weather_settings(monkeypatch):
    prod = load_settings("prod")
    assert len(prod.weather.stations) == 34 and {"LFPG", "EGLL", "LFBO", "ENZV", "EDDF"} <= set(prod.weather.stations)
    assert (prod.weather.vmc_visibility_m, prod.weather.vmc_ceiling_ft) == (5000.0, 1500.0)     # ICAO VFR minima
    assert (prod.weather.cat1_visibility_m, prod.weather.cat1_ceiling_ft) == (800.0, 200.0)     # ICAO CAT I
    monkeypatch.setenv("OPDI_WEATHER_STATIONS", "LFBO,EGLL")
    monkeypatch.setenv("OPDI_WEATHER_URL", "http://127.0.0.1:1/iem/asos.py")
    s = load_settings("dev")
    assert s.weather.stations == ("LFBO", "EGLL") and s.weather.base_url.startswith("http://127.0.0.1")
    assert load_settings("dev", weather_stations="ENZV").weather.stations == ("ENZV",)   # argument > environment
