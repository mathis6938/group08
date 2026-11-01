"""Small helpers shared by the layers: time, disk, JSON reports, timing of the steps."""
import datetime
import json
import logging
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger("opdi")


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def free_gb(path):
    """Free disk space (GB) on the disk that holds `path` (or its closest existing parent)."""
    p = Path(path)
    while not p.exists():
        p = p.parent
    return shutil.disk_usage(p).free / 1e9


def parquet_gb(path):
    p = Path(path)
    return sum(f.stat().st_size for f in p.rglob("*.parquet")) / 1e9 if p.exists() else 0.0


def table_rows(spark, path):
    p = Path(path)
    return spark.read.parquet(str(p)).count() if p.exists() and any(p.rglob("*.parquet")) else 0


def write_table(df, path, partition_cols=None, files=None):
    """Full rewrite of a table (static overwrite: partitions that no longer exist are removed too)."""
    writer = (df.coalesce(files) if files else df).write.mode("overwrite").option("partitionOverwriteMode", "static")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(str(path))


class Steps:
    """Durations of the steps of a layer, kept in its report (used to find the pipeline bottleneck)."""

    def __init__(self):
        self.durations = {}

    @contextmanager
    def step(self, name):
        log.info("-> %s", name)
        t0 = time.time()
        try:
            yield
        finally:
            self.durations[name] = round(time.time() - t0, 1)
            log.info("   %s done in %.1f s", name, self.durations[name])


def write_report(settings, layer, report):
    """Report of one layer for this run: <data root>/reports/<run id>/<layer>.json."""
    settings.report_dir.mkdir(parents=True, exist_ok=True)
    report = {"layer": layer, "run_id": settings.run_id, "env": settings.env,
              "scope_start": str(settings.start), "scope_end": str(settings.end),
              "finished_at_utc": utc_now().isoformat(timespec="seconds"), **report}
    path = settings.report_dir / f"{layer}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    return report


def read_report(settings, layer):
    path = settings.report_dir / f"{layer}.json"
    return json.loads(path.read_text()) if path.exists() else None


class QualityGateError(RuntimeError):
    """A quality check failed: the run must stop (the next layers must not use these data)."""
