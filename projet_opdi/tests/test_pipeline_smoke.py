"""End-to-end test: the whole notebook runs on a small synthetic OPDI dataset served by a fake EUROCONTROL server.

It checks that every layer runs and that the pipeline finds exactly the defects put in the synthetic data.
"""
import functools
import http.server
import json
import os
import sys
import threading
from pathlib import Path

import pytest

import run_project
from synthetic_opdi import START, END, write_site

STUBS = Path(__file__).resolve().parent / "stubs"


@pytest.fixture(scope="module")
def pipeline_run(tmp_path_factory):
    site = tmp_path_factory.mktemp("site")
    data_root = tmp_path_factory.mktemp("data")
    expected = write_site(site)

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(site))
    handler.log_message = lambda *a, **k: None
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"

    settings = {
        "OPDI_DATA_ROOT": str(data_root), "OPDI_START": str(START), "OPDI_END": str(END),
        "OPDI_BASE_URL": f"{url}/opdi",
        "OPDI_AIRPORTS_URL": f"{url}/ourairports/airports.csv",
        "OPDI_RUNWAYS_URL": f"{url}/ourairports/runways.csv",
        # the notebook kernel: findspark stand-in, Spark workers on the same Python
        "PYTHONPATH": os.pathsep.join([str(STUBS), os.environ.get("PYTHONPATH", "")]),
        "PYSPARK_PYTHON": sys.executable,
    }
    saved = {k: os.environ.get(k) for k in settings}
    try:
        ok, report, error = run_project.run_notebook(settings, data_root / "reports")
    finally:
        httpd.shutdown()
        for k, v in saved.items():                     # leave the test process environment unchanged
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    assert ok, f"notebook failed (report: {report}):\n{error[-4000:]}"
    return data_root, report, expected


def read(spark, path):
    return spark.read.parquet(str(path))


@pytest.mark.slow
def test_notebook_runs_and_publishes(pipeline_run):
    data_root, report, _ = pipeline_run
    assert report.exists() and report.with_suffix(".html").exists()
    summary = json.loads((data_root / "pipeline_summary.json").read_text())
    assert summary["status"] == "PASSED", summary["gates"]
    assert all(summary["gates"].values())
    assert (data_root / "gold" / "_SUCCESS").exists()


@pytest.mark.slow
def test_bronze_keeps_every_row_with_lineage(pipeline_run, spark):
    data_root, _, exp = pipeline_run
    flights = read(spark, data_root / "bronze" / "flight_list")
    assert flights.count() == exp["bronze_flights"]
    assert flights.filter("_source_sha256 IS NULL OR _source_file IS NULL").count() == 0
    assert read(spark, data_root / "bronze" / "flight_events").count() == exp["bronze_events"]
    manifest = (data_root / "raw" / "manifest.csv").read_text()
    assert manifest.count("downloaded") == 5                     # 3 OPDI files + 2 reference files


@pytest.mark.slow
def test_silver_finds_the_planted_defects(pipeline_run, spark):
    data_root, _, exp = pipeline_run
    silver_fl = read(spark, data_root / "silver" / "flights")
    assert silver_fl.count() == exp["valid_flights"]               # duplicates and out-of-scope flights removed
    assert silver_fl.select("flight_id").distinct().count() == exp["valid_flights"]
    assert silver_fl.filter("callsign LIKE ' %' OR callsign LIKE '% '").count() == 0     # trimmed
    assert silver_fl.filter("origin_icao = '' OR origin_icao = 'NAN'").count() == 0     # one form of missing
    assert silver_fl.filter("NOT has_adep").count() == exp["valid_flights"] // 10       # flagged, kept

    q_fl = read(spark, data_root / "quarantine" / "flights")
    assert {r[0]: r[1] for r in q_fl.groupBy("_quarantine_reason").count().collect()} == exp["quarantine_flights"]

    q_ev = {r[0]: r[1] for r in read(spark, data_root / "quarantine" / "flight_events")
            .groupBy("_quarantine_reason").count().collect()}
    assert q_ev == {"ORPHAN_EVENT": exp["orphan_events"], "INVALID_COORDINATES": exp["invalid_coordinate_events"]}

    silver_ev = read(spark, data_root / "silver" / "flight_events")
    assert silver_ev.count() == exp["valid_events"]                # duplicates and out-of-scope events removed
    assert silver_ev.join(silver_fl, "flight_id", "left_anti").count() == 0
    assert dict(silver_ev.dtypes)["event_timestamp_utc"] == "timestamp"


@pytest.mark.slow
def test_gold_figures(pipeline_run, spark):
    data_root, _, exp = pipeline_run
    daily = read(spark, data_root / "gold" / "airport_daily_ops")
    tot = daily.agg({"departures": "sum", "arrivals": "sum"}).first()
    known_origin = exp["valid_flights"] - exp["valid_flights"] // 10
    assert (tot["sum(departures)"], tot["sum(arrivals)"]) == (known_origin, exp["valid_flights"])

    runway = read(spark, data_root / "gold" / "runway_usage")
    assert runway.agg({"runway_movements": "sum"}).first()[0] == known_origin + exp["valid_flights"]
    assert runway.filter("length_ft IS NULL").count() == 0        # every OSM reference matched a runway

    ground = read(spark, data_root / "gold" / "airport_ground_times").filter("airport_icao = 'LFBO'")
    row = ground.agg({"avg_taxi_out_min": "avg", "avg_taxi_in_min": "avg"}).first()
    assert row["avg(avg_taxi_out_min)"] == pytest.approx(10.0)    # exit-parking -> take-off = 10 min
    assert row["avg(avg_taxi_in_min)"] == pytest.approx(6.0)      # landing -> entry-parking = 6 min
