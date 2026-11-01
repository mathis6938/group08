"""End-to-end summary of a run: quality gates of every layer, volumes and storage, quarantine reasons, step
durations. Written to <data root>/pipeline_summary.json and shown in the GitHub Actions job summary."""
import json
import os

from .common import free_gb, log, parquet_gb, read_report, table_rows, utc_now, write_report

LAYER_TABLES = [
    ("Bronze", "bronze_dir", ["flight_list", "flight_events", "measurements", "airports", "runways"]),
    ("Silver", "silver_dir", ["flights", "flight_events", "airports", "runways"]),
    ("Quarantine", "quarantine_dir", ["flights", "flight_events"]),
    ("Gold", "gold_dir", ["airport_hourly_ops", "airport_daily_ops", "runway_usage", "airport_ground_times"]),
]


def gates(settings):
    """Result of the checks made in each layer of this run (a layer that did not run counts as failed)."""
    bronze, silver, gold = (read_report(settings, layer) for layer in ("bronze", "silver", "gold"))
    return {
        "Bronze: raw -> Bronze reconciliation": bool(bronze and bronze["reconciliation_passed"]),
        "Silver: row accounting consistent": bool(silver and silver["accounting_consistent"]),
        "Silver: no orphan event": bool(silver and silver["orphan_events"] == 0),
        "Gold: validation checks": bool(gold and gold["validation_passed"]),
        "Gold: published (_SUCCESS marker)": (settings.gold_dir / "_SUCCESS").exists(),
    }


def run(spark, settings):
    tables = []
    for layer, attr, names in LAYER_TABLES:
        root = getattr(settings, attr)
        for name in names:
            tables.append(dict(layer=layer, table=name, rows=table_rows(spark, root / name),
                               size_gb=round(parquet_gb(root / name), 4)))
    log.info("%-11s %-22s %14s %10s", "Layer", "Table", "Rows", "Size (GB)")
    for t in tables:
        log.info("%-11s %-22s %14s %10.3f", t["layer"], t["table"], f"{t['rows']:,}", t["size_gb"])

    reports = {layer: read_report(settings, layer) for layer in ("ingest", "bronze", "silver", "gold")}
    durations = {f"{layer}: {step}": seconds for layer, r in reports.items() if r
                 for step, seconds in r.get("durations_s", {}).items()}
    slowest = sorted(durations.items(), key=lambda kv: kv[1], reverse=True)[:5]
    log.info("Slowest steps of the run: %s", slowest)

    gate_results = gates(settings)
    for name, ok in gate_results.items():
        log.info("  %s  %s", "PASSED" if ok else "FAILED", name)
    passed = all(gate_results.values())
    log.info("END-TO-END PIPELINE: %s", "PASSED" if passed else "FAILED")

    silver = reports["silver"] or {}
    summary = dict(status="PASSED" if passed else "FAILED", run_id=settings.run_id, env=settings.env,
                   generated_at_utc=utc_now().isoformat(timespec="seconds"), scope_start=str(settings.start),
                   scope_end=str(settings.end), data_root=str(settings.data_root), gates=gate_results,
                   tables=tables, quarantine_reasons=silver.get("quarantine_reasons", {}),
                   step_durations_s=durations, free_disk_gb=round(free_gb(settings.data_root), 1))
    (settings.data_root / "pipeline_summary.json").write_text(json.dumps(summary, indent=2))
    write_report(settings, "summary", summary)
    write_github_summary(summary)
    return summary


def write_github_summary(s):
    """Markdown summary of the run in the GitHub Actions job summary (only when running in GitHub Actions)."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = [f"## OPDI pipeline — {s['env']} (run {s['run_id']})", "",
             f"**Result:** {'✅ PASSED' if s['status'] == 'PASSED' else '❌ FAILED'} | scope "
             f"[{s['scope_start']}, {s['scope_end']}[ | data `{s['data_root']}` | free disk {s['free_disk_gb']} GB", "",
             "### Quality gates", "", "| Gate | Result |", "|---|---|"]
    lines += [f"| {name} | {'✅' if ok else '❌'} |" for name, ok in s["gates"].items()]
    lines += ["", "### Tables", "", "| Layer | Table | Rows | Size (GB) |", "|---|---|---:|---:|"]
    lines += [f"| {t['layer']} | {t['table']} | {t['rows']:,} | {t['size_gb']:.3f} |" for t in s["tables"]]
    if s.get("quarantine_reasons"):
        lines += ["", "### Quarantine reasons", "", "| Table | Reason | Rows |", "|---|---|---:|"]
        lines += [f"| {table} | {reason} | {n:,} |"
                  for table, reasons in s["quarantine_reasons"].items() for reason, n in reasons.items()]
    if s.get("step_durations_s"):
        lines += ["", "### Slowest steps", "", "| Step | Duration (s) |", "|---|---:|"]
        lines += [f"| {step} | {sec} |" for step, sec in
                  sorted(s["step_durations_s"].items(), key=lambda kv: kv[1], reverse=True)[:8]]
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
