#!/usr/bin/env python3
"""
Run the OPDI Medallion pipeline notebook headlessly (used by GitHub Actions).

The notebook is the single source of the pipeline code. This script executes it from top to bottom
with the settings of one environment, saves the executed notebook (with all outputs) as a run report,
and fails if one cell fails (the notebook raises an error when a quality gate fails).

Usage (on the VM, in ~/spark-env):
    python projet_opdi/run_project.py --env dev                 # one 10-day window, ~/opdi-data-dev
    python projet_opdi/run_project.py --env prod                # full scope, ~/opdi-data
    python projet_opdi/run_project.py --env prod --start 2026-06-01 --end 2026-06-11
"""
import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
NOTEBOOK = PROJECT_DIR / "src" / "opdi_pipeline" / "project_pipeline_en_construction.ipynb"

# Settings of each environment, passed to the notebook as environment variables (read in Part 1.3)
ENVIRONMENTS = {
    "dev":  {"OPDI_DATA_ROOT": "~/opdi-data-dev", "OPDI_START": "2026-06-09", "OPDI_END": "2026-06-19"},
    "prod": {"OPDI_DATA_ROOT": "~/opdi-data",     "OPDI_START": "2026-06-01", "OPDI_END": "2026-07-29"},
}


def run_notebook(settings, report_dir, kernel_name="python3"):
    """Execute the notebook with `settings` as environment variables.

    Returns (ok, executed_notebook_path, error_message). The executed notebook is saved even if a cell fails.
    """
    import nbformat
    from nbclient import NotebookClient
    from nbclient.exceptions import CellExecutionError

    os.environ.update(settings)                      # inherited by the notebook kernel
    nb = nbformat.read(NOTEBOOK, as_version=4)
    client = NotebookClient(nb, kernel_name=kernel_name, timeout=None,  # no time limit per cell
                            resources={"metadata": {"path": str(NOTEBOOK.parent)}})
    ok, error = True, ""
    try:
        client.execute()
    except CellExecutionError as e:
        ok, error = False, str(e)
    finally:
        report_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = report_dir / f"pipeline_run_{stamp}.ipynb"
        nbformat.write(nb, out)
        try:                                         # HTML copy, easier to read in the artifacts
            from nbconvert import HTMLExporter
            html, _ = HTMLExporter().from_notebook_node(nb)
            out.with_suffix(".html").write_text(html, encoding="utf-8")
        except Exception as e:                       # the report is a convenience: never fail on it
            print(f"(HTML report not written: {e})")
    return ok, out, error


def write_github_summary(env, settings, ok, elapsed_s, data_root):
    """Append a Markdown summary of the run to the GitHub Actions job summary (if running in Actions)."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    summary_file = data_root / "pipeline_summary.json"
    s = json.loads(summary_file.read_text()) if summary_file.exists() else None
    lines = [f"## OPDI pipeline — {env}", "",
             f"**Result:** {'✅ PASSED' if ok else '❌ FAILED'} | scope [{settings['OPDI_START']}, {settings['OPDI_END']}[ "
             f"| duration {elapsed_s / 60:.1f} min | data `{data_root}`", ""]
    if s:
        lines += ["### Quality gates", "", "| Gate | Result |", "|---|---|"]
        lines += [f"| {name} | {'✅' if passed else '❌'} |" for name, passed in s["gates"].items()]
        lines += ["", "### Tables", "", "| Layer | Table | Rows | Size (GB) |", "|---|---|---:|---:|"]
        lines += [f"| {t['layer']} | {t['table']} | {t['rows']:,} | {t['size_gb']:.3f} |" for t in s["tables"]]
        if s.get("quarantine_reasons"):
            lines += ["", "### Quarantine reasons", "", "| Table | Reason | Rows |", "|---|---|---:|"]
            lines += [f"| {table} | {reason} | {n:,} |"
                      for table, reasons in s["quarantine_reasons"].items() for reason, n in reasons.items()]
    else:
        lines += ["The pipeline stopped before Part 5: see the executed notebook in the artifacts."]
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Run the OPDI Medallion pipeline notebook")
    parser.add_argument("--env", choices=sorted(ENVIRONMENTS), required=True)
    parser.add_argument("--start", help="override the first day of the scope (YYYY-MM-DD)")
    parser.add_argument("--end", help="override the end of the scope, exclusive (YYYY-MM-DD)")
    parser.add_argument("--report-dir", help="where to save the executed notebook (default: <data root>/reports)")
    args = parser.parse_args()

    settings = dict(ENVIRONMENTS[args.env])
    if args.start:
        settings["OPDI_START"] = args.start
    if args.end:
        settings["OPDI_END"] = args.end
    for key in ("OPDI_START", "OPDI_END"):
        datetime.date.fromisoformat(settings[key])   # fail early on a malformed date
    data_root = Path(settings["OPDI_DATA_ROOT"]).expanduser()
    settings["OPDI_DATA_ROOT"] = str(data_root)
    report_dir = Path(args.report_dir) if args.report_dir else data_root / "reports"

    print(f"Running {NOTEBOOK.name} | env={args.env} | " + " ".join(f"{k}={v}" for k, v in settings.items()))
    (data_root / "pipeline_summary.json").unlink(missing_ok=True)   # never report the summary of a previous run
    t0 = time.time()
    ok, report, error = run_notebook(settings, report_dir)
    elapsed = time.time() - t0
    print(f"Executed notebook saved to {report}")
    write_github_summary(args.env, settings, ok, elapsed, data_root)
    if not ok:
        print("PIPELINE FAILED:\n" + error[-3000:])
        return 1
    print(f"PIPELINE PASSED in {elapsed / 60:.1f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
