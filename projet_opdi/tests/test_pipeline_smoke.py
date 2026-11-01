"""End-to-end test: the whole pipeline runs on a small synthetic OPDI dataset served by a fake EUROCONTROL server,
layer by layer through the command line, exactly as in GitHub Actions.

It checks that every layer runs and that the pipeline finds exactly the defects put in the synthetic data.
"""
import functools
import http.server
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from synthetic_opdi import START, END, write_site

RUN_PIPELINE = Path(__file__).resolve().parents[1] / "run_pipeline.py"
RUN_ID = "test-run"


def run_cli(layers, env):
    """python run_pipeline.py --env dev --layer <layers>, as a separate process (like a CI step)."""
    proc = subprocess.run([sys.executable, str(RUN_PIPELINE), "--env", "dev", "--layer", layers],
                          env=env, capture_output=True, text=True, timeout=900)
    return proc.returncode, proc.stdout + proc.stderr


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

    env = {**os.environ,
           "OPDI_DATA_ROOT": str(data_root), "OPDI_START": str(START), "OPDI_END": str(END), "OPDI_RUN_ID": RUN_ID,
           "OPDI_BASE_URL": f"{url}/opdi",
           "OPDI_AIRPORTS_URL": f"{url}/ourairports/airports.csv",
           "OPDI_RUNWAYS_URL": f"{url}/ourairports/runways.csv",
           "PYSPARK_PYTHON": sys.executable}
    logs = {}
    for layer in ("ingest", "bronze", "silver", "gold", "summary"):     # one process per layer, as in the CI
        code, logs[layer] = run_cli(layer, env)
        assert code == 0, f"layer {layer} failed:\n{logs[layer][-4000:]}"
    yield data_root, env, expected, logs
    httpd.shutdown()


def read(spark, path):
    return spark.read.parquet(str(path))


@pytest.mark.slow
def test_pipeline_runs_and_publishes(pipeline_run):
    data_root, _, _, logs = pipeline_run
    for layer in ("ingest", "bronze", "silver", "gold", "summary"):
        assert (data_root / "reports" / RUN_ID / f"{layer}.json").exists()
    assert "END-TO-END PIPELINE: PASSED" in logs["summary"]
    summary = json.loads((data_root / "pipeline_summary.json").read_text())
    assert summary["status"] == "PASSED", summary["gates"]
    assert all(summary["gates"].values())
    assert (data_root / "gold" / "_SUCCESS").exists()


@pytest.mark.slow
def test_bronze_keeps_every_row_with_lineage(pipeline_run, spark):
    data_root, _, exp, _ = pipeline_run
    flights = read(spark, data_root / "bronze" / "flight_list")
    assert flights.count() == exp["bronze_flights"]
    assert flights.filter("_source_sha256 IS NULL OR _source_file IS NULL").count() == 0
    assert read(spark, data_root / "bronze" / "flight_events").count() == exp["bronze_events"]
    manifest = (data_root / "raw" / "manifest.csv").read_text()
    assert manifest.count("downloaded") == 5                     # 3 OPDI files + 2 reference files


@pytest.mark.slow
def test_silver_finds_the_planted_defects(pipeline_run, spark):
    data_root, _, exp, _ = pipeline_run
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
    data_root, _, exp, _ = pipeline_run
    daily = read(spark, data_root / "gold" / "airport_daily_ops")
    tot = daily.agg({"departures": "sum", "arrivals": "sum"}).first()
    known_origin = exp["valid_flights"] - exp["valid_flights"] // 10
    assert (tot["sum(departures)"], tot["sum(arrivals)"]) == (known_origin, exp["valid_flights"])

    runway = read(spark, data_root / "gold" / "runway_usage")
    assert runway.agg({"runway_movements": "sum"}).first()[0] == known_origin + exp["valid_flights"]
    assert runway.filter("length_ft IS NULL").count() == 0        # "14L/32R" and "14L-32R" both matched

    ground = read(spark, data_root / "gold" / "airport_ground_times").filter("airport_icao = 'LFBO'")
    row = ground.agg({"avg_taxi_out_min": "avg", "avg_taxi_in_min": "avg"}).first()
    assert row["avg(avg_taxi_out_min)"] == pytest.approx(9.0)     # exit-parking -> runway line-up = 9 min
    assert row["avg(avg_taxi_in_min)"] == pytest.approx(5.0)      # runway exit -> entry-parking = 5 min


@pytest.mark.slow
def test_published_tables_match_their_schema_contract(pipeline_run, spark):
    from opdi_pipeline import schemas
    data_root, _, _, _ = pipeline_run
    for layer, contracts in [("silver", schemas.SILVER), ("gold", schemas.GOLD)]:
        for table, contract in contracts.items():
            assert schemas.compare(read(spark, data_root / layer / table).schema, contract) == [], (layer, table)
    gold = json.loads((data_root / "reports" / RUN_ID / "gold.json").read_text())
    assert all(v for k, v in gold["checks"].items() if "schema" in k)


@pytest.mark.slow
def test_rerun_is_idempotent(pipeline_run, spark):
    """A second run on the same data gives the same tables (no duplicates, nothing downloaded again)."""
    data_root, env, exp, _ = pipeline_run
    before = json.loads((data_root / "pipeline_summary.json").read_text())["tables"]
    code, out = run_cli("all", {**env, "OPDI_RUN_ID": "test-rerun"})
    assert code == 0, out[-4000:]
    after = json.loads((data_root / "pipeline_summary.json").read_text())["tables"]
    assert [(t["table"], t["rows"]) for t in after] == [(t["table"], t["rows"]) for t in before]
    ingest = json.loads((data_root / "reports" / "test-rerun" / "ingest.json").read_text())
    assert ingest["results"] == {"already_present": 5}


@pytest.mark.slow
def test_failed_quality_gate_stops_the_run(pipeline_run):
    """If a Bronze file loses its lineage (no valid manifest entry), the reconciliation fails and the run stops."""
    data_root, env, _, _ = pipeline_run
    manifest = data_root / "raw" / "manifest.csv"
    original = manifest.read_text()
    lines = original.splitlines()
    manifest.write_text("\n".join(lines[:1] + [l.replace(",already_present,", ",failed,").replace(",downloaded,", ",failed,")
                                              if "flight_list_" in l else l for l in lines[1:]]) + "\n")
    try:
        code, out = run_cli("bronze,silver", {**env, "OPDI_RUN_ID": "test-fail"})
    finally:
        manifest.write_text(original)
    assert code == 1 and "QUALITY GATE FAILED" in out
    assert not (data_root / "reports" / "test-fail" / "silver.json").exists()      # Silver did not run
