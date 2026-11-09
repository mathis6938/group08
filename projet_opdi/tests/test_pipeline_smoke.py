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
import time
from pathlib import Path

import pytest

from synthetic_opdi import START, END, write_site

RUN_PIPELINE = Path(__file__).resolve().parents[1] / "run_pipeline.py"
RUN_ID = "test-run"


def run_cli(layers, env, *extra):
    """python run_pipeline.py --env dev --layer <layers>, as a separate process (like a CI step)."""
    proc = subprocess.run([sys.executable, str(RUN_PIPELINE), "--env", "dev", "--layer", layers, *extra],
                          env=env, capture_output=True, text=True, timeout=900)
    return proc.returncode, proc.stdout + proc.stderr


def report(data_root, run_id, layer):
    return json.loads((data_root / "reports" / run_id / f"{layer}.json").read_text())


@pytest.fixture(scope="module")
def site(tmp_path_factory):
    return tmp_path_factory.mktemp("site")


@pytest.fixture(scope="module")
def pipeline_run(tmp_path_factory, site):
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
    for layer in ("ingest", "bronze", "silver", "gold", "ml", "summary"):     # one process per layer, as in the CI
        code, logs[layer] = run_cli(layer, env)
        assert code == 0, f"layer {layer} failed:\n{logs[layer][-4000:]}"
    yield data_root, env, expected, logs
    httpd.shutdown()


def read(spark, path):
    return spark.read.parquet(str(path))


@pytest.mark.slow
def test_pipeline_runs_and_publishes(pipeline_run):
    data_root, _, _, logs = pipeline_run
    for layer in ("ingest", "bronze", "silver", "gold", "ml", "summary"):
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
    bronze = report(data_root, RUN_ID, "bronze")
    assert len(bronze["files_built"]) == 5 and bronze["files_up_to_date"] == 0     # first run: everything built


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

    # Arrival efficiency Silver tables
    arr = read(spark, data_root / "silver" / "arrivals")
    assert arr.count() == exp["arrivals"]
    assert arr.filter("is_transit_plausible").count() == exp["measured_arrivals"]
    assert arr.filter("holding_count > 0").count() == exp["holdings"]
    # Check that entry sector is assigned (due north in synthetic data)
    assert arr.filter("entry_sector = 'N'").count() == exp["measured_arrivals"]

    lev = read(spark, data_root / "silver" / "descent_level_offs")
    assert lev.count() == exp["fused_level_offs"]
    assert lev.filter("is_holding").count() == exp["holdings"]
    # Ground levels (alt < 1500 ft) excluded
    assert lev.filter("altitude_ft < 1500").count() == 0
    # Chunked level segments fused
    assert lev.filter("segments_merged > 1").count() == 20
    assert lev.filter("segments_merged > 1").first()["segments_merged"] == 2


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

    # Arrival efficiency Gold tables
    arr_daily = read(spark, data_root / "gold" / "airport_arrival_daily")
    assert arr_daily.count() > 0
    assert arr_daily.filter("arrivals < arrivals_measured").count() == 0
    assert arr_daily.filter("NOT pct_coverage BETWEEN 0 AND 100").count() == 0

    arr_ref = read(spark, data_root / "gold" / "arrival_reference")
    assert arr_ref.count() > 0
    assert arr_ref.filter("ref_transit_min <= 0").count() == 0

    hotspots = read(spark, data_root / "gold" / "holding_hotspots")
    assert hotspots.count() > 0
    assert hotspots.agg({"holdings": "sum"}).first()[0] == exp["holdings"]

    # Check incomplete hours detection in gold report
    gold_rep = report(data_root, RUN_ID, "gold")
    assert any(h["movement_hour"] == 14 and "2026-06-09" in str(h["movement_date"])
               for h in gold_rep.get("incomplete_hours", []))

    # Check ML layer report and published tables
    ml_rep = report(data_root, RUN_ID, "ml")
    assert ml_rep["status"] in ("PASSED", "SKIPPED")
    if ml_rep["status"] == "PASSED":
        assert (data_root / "gold" / "ml_feature_importance").exists()
        assert (data_root / "gold" / "ml_scenarios").exists()
        assert (data_root / "gold" / "airport_typology").exists()
        assert read(spark, data_root / "gold" / "airport_typology").count() > 0


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
def test_explore_tool(pipeline_run):
    """On-demand exploration of the arrival phase: runs on the published tables and finds the planted descents."""
    data_root, env, _, _ = pipeline_run
    code, out = run_cli("explore", {**env, "OPDI_RUN_ID": "test-explore"})
    assert code == 0, out[-4000:]
    r = report(data_root, "test-explore", "explore")
    assert r["errors"] == {}, r["errors"]
    assert {m["type"] for m in r["measurements"]["by_type"]} == {"Time Passed (s)", "Distance flown (NM)"}
    lfbo = next(c for c in r["milestones"]["coverage"] if c["dest_icao"] == "LFBO")
    assert lfbo["pct_tod"] == 100.0 and lfbo["pct_rwy"] == 100.0
    assert lfbo["med_tod_km"] == pytest.approx(150, abs=2) and lfbo["med_fl100_km"] == pytest.approx(50, abs=2)
    lv = r["levels"]
    assert lv["pct_paired_with_level_end"] == 100.0
    assert lv["paired_duration_min"]["p50"] == pytest.approx(6.0)               # the planted holding patterns
    assert lv["long_segment_ratio_flown_displacement"]["p50"] > 100            # flown 24 NM, displacement ~0
    assert r["sequences"]["flights"]


@pytest.mark.slow
def test_rerun_is_idempotent(pipeline_run, spark):
    """A second run on the same data gives the same tables (no duplicates, nothing downloaded again)."""
    data_root, env, exp, _ = pipeline_run
    before = json.loads((data_root / "pipeline_summary.json").read_text())["tables"]
    code, out = run_cli("all", {**env, "OPDI_RUN_ID": "test-rerun"})
    assert code == 0, out[-4000:]
    after = json.loads((data_root / "pipeline_summary.json").read_text())["tables"]
    assert [(t["table"], t["rows"]) for t in after] == [(t["table"], t["rows"]) for t in before]
    assert report(data_root, "test-rerun", "ingest")["results"] == {"already_present": 5}
    bronze = report(data_root, "test-rerun", "bronze")
    assert bronze["files_built"] == [] and bronze["files_up_to_date"] == 5        # incremental: nothing rebuilt
    assert bronze["reconciliation_passed"]


@pytest.mark.slow
def test_republished_file_rebuilds_only_its_period(pipeline_run, site, spark):
    """EUROCONTROL publishes a new version of one file: only that file is downloaded again and only its Bronze
    period is rewritten; the other periods keep their rows and lineage, and the reconciliation still passes."""
    import pyarrow.parquet as pq
    data_root, env, exp, _ = pipeline_run
    src = next((site / "opdi" / "flight_events").glob("*.parquet"))
    pq.write_table(pq.read_table(src), src, compression="gzip")       # same rows, other bytes (new version)
    later = time.time() + 120
    os.utime(src, (later, later))

    code, out = run_cli("ingest,bronze", {**env, "OPDI_RUN_ID": "test-republish"})
    assert code == 0, out[-4000:]
    assert report(data_root, "test-republish", "ingest")["results"] == {"republished": 1, "already_present": 4}
    bronze = report(data_root, "test-republish", "bronze")
    assert bronze["files_built"] == [src.name] and bronze["files_up_to_date"] == 4
    assert bronze["reconciliation_passed"]

    events = read(spark, data_root / "bronze" / "flight_events")
    assert events.count() == exp["bronze_events"]                   # partition replaced, not appended
    assert {r[0] for r in events.select("_bronze_run_id").distinct().collect()} == {"test-republish"}
    assert {r[0] for r in read(spark, data_root / "bronze" / "flight_list")
            .select("_bronze_run_id").distinct().collect()} == {RUN_ID}       # untouched
    sha = ingestion_sha(data_root, src.name)
    assert {r[0] for r in events.select("_source_sha256").distinct().collect()} == {sha}   # lineage of the new version

    code, out = run_cli("bronze", {**env, "OPDI_RUN_ID": "test-full-refresh"}, "--full-refresh")
    assert code == 0, out[-4000:]
    bronze = report(data_root, "test-full-refresh", "bronze")
    assert bronze["full_refresh"] and len(bronze["files_built"]) == 5 and bronze["reconciliation_passed"]
    assert read(spark, data_root / "bronze" / "flight_events").count() == exp["bronze_events"]


def ingestion_sha(data_root, name):
    import csv
    with open(data_root / "raw" / "manifest.csv", newline="") as f:
        return [r["sha256"] for r in csv.DictReader(f) if r["file"] == name][-1]


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
    assert not (data_root / "bronze" / "_state.json").exists()     # unknown state: the next run rebuilds everything
