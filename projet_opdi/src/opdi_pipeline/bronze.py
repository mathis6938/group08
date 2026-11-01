"""Bronze: schema check of the raw files, Bronze tables (original columns and types + row-level lineage),
and reconciliation of the row counts between raw/ and bronze/."""
import csv
import hashlib
from collections import defaultdict
from pathlib import Path

from pyspark.sql.functions import broadcast, col, count, current_timestamp, input_file_name, lit, regexp_extract
from pyspark.sql.functions import sum as spark_sum

from . import ingestion
from .common import QualityGateError, Steps, free_gb, log, parquet_gb, utc_now, write_report
from .config import PARQUET_TABLES

LINEAGE_COLUMNS = ["_source_file", "_source_period", "_ingestion_run_id", "_ingested_at_utc",
                   "_source_last_modified", "_source_sha256", "_bronze_run_id", "_bronze_loaded_at_utc"]


def read_reference_csv(spark, path):
    """OurAirports CSV: header, quoted fields that may contain commas and line breaks; every column kept as text."""
    return spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(str(path))


# ---------------------------------------------------------------------------------------------- schemas
def check_schemas(spark, settings):
    """Schema fingerprint and row count of every raw Parquet file (raw/schema_report.csv), grouped by variant.

    OPDI files from different periods were observed with different schemas (e.g. `long` vs `decimal(20,0)` ids,
    dates as `string` vs `timestamp`): all the files of a table must share one schema before Silver.
    """
    variants = defaultdict(lambda: defaultdict(list))             # table -> schema -> files
    rows = []
    for table in PARQUET_TABLES:
        for path in sorted((settings.raw_dir / table).glob("*.parquet")):
            df = spark.read.parquet(str(path))                     # reads the Parquet metadata only
            schema = tuple((f.name, f.dataType.simpleString()) for f in df.schema.fields)
            fingerprint = hashlib.sha256(str(schema).encode()).hexdigest()[:12]
            variants[table][schema].append(path.name)
            rows.append(dict(table=table, file=path.name, n_rows=df.count(), n_columns=len(schema),
                             schema_fingerprint=fingerprint, checked_at_utc=utc_now().isoformat(timespec="seconds")))

    for table, by_schema in variants.items():
        keys = list(by_schema)
        log.info("=== %s: %d files | %d schema variant(s) ===", table, sum(map(len, by_schema.values())), len(keys))
        for k, key in enumerate(keys, 1):
            names = by_schema[key]
            listed = ", ".join(names) if len(names) <= 3 else f"{names[0]} ... {names[-1]}"
            log.info("  Variant %d: %d file(s) -> %s", k, len(names), listed)
        if len(keys) == 1:
            for name, dtype in keys[0]:
                log.info("      %-22s %s", name, dtype)
        else:                                                      # several schemas: show the differences only
            types = [dict(key) for key in keys]
            for c in dict.fromkeys(c for t in types for c in t):
                vals = [t.get(c, "-- missing --") for t in types]
                if len(set(vals)) > 1:
                    log.info("      %-22s %s", c, " | ".join(f"V{i + 1}: {v}" for i, v in enumerate(vals)))
    for r in rows:
        log.info("  %-13s %-45s %12s rows  schema %s", r["table"], r["file"], f"{r['n_rows']:,}", r["schema_fingerprint"])

    if rows:
        with open(settings.schema_report_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    return {table: len(by_schema) for table, by_schema in variants.items()}, rows


# ---------------------------------------------------------------------------------------------- tables
def file_lineage(spark, settings):
    """Latest successful manifest entry of each file (the manifest is chronological: the last entry wins)."""
    latest = {}
    with open(settings.manifest_path, newline="") as f:
        for r in csv.DictReader(f):
            if r["result"] in ingestion.OK_RESULTS:
                latest[r["file"]] = r
    return spark.createDataFrame(
        [(name, r["run_id"], r["retrieved_at_utc"], r["last_modified"], r["sha256"]) for name, r in latest.items()],
        "_source_file string, _ingestion_run_id string, _ingested_at_utc string, "
        "_source_last_modified string, _source_sha256 string",
    ).withColumn("_ingested_at_utc", col("_ingested_at_utc").cast("timestamp"))


def add_lineage(df, lineage, bronze_run_id):
    """Row-level lineage: each row gets its source file name, its period, and the file metadata of the manifest."""
    return (df
        .withColumn("_source_file", regexp_extract(input_file_name(), r"([^/]+)$", 1))
        .withColumn("_source_period", regexp_extract(col("_source_file"), r"_(\d{6,8})(?:_\d{8})?\.parquet$", 1))
        .join(broadcast(lineage), "_source_file", "left")          # small table: broadcast join, no shuffle
        .withColumn("_bronze_run_id", lit(bronze_run_id))
        .withColumn("_bronze_loaded_at_utc", current_timestamp()))


def build_tables(spark, settings, files):
    """Bronze tables: original columns and types unchanged + lineage. OPDI tables partitioned by source period."""
    lineage = file_lineage(spark, settings)
    for table in PARQUET_TABLES:
        (add_lineage(spark.read.parquet(*files[table]), lineage, settings.run_id)
            .write.mode("overwrite")                               # dynamic: only the periods written are replaced
            .partitionBy("_source_period")
            .parquet(str(settings.bronze_dir / table)))
        log.info("%-13s -> %s", table, settings.bronze_dir / table)
    for path in files.get("reference", []):                        # airports, runways: every column kept as text
        table = Path(path).stem
        (add_lineage(read_reference_csv(spark, path), lineage, settings.run_id).drop("_source_period")
            .write.mode("overwrite").parquet(str(settings.bronze_dir / table)))
        log.info("%-13s -> %s", table, settings.bronze_dir / table)


def reconcile(spark, settings, files):
    """Every raw row must be in Bronze, with its lineage: row counts compared file by file."""
    expected = {}
    with open(settings.schema_report_path, newline="") as f:      # Parquet files: counted by check_schemas
        for r in csv.DictReader(f):
            expected[r["file"]] = int(r["n_rows"])
    for path in files.get("reference", []):                        # CSV files: counted directly
        expected[Path(path).name] = read_reference_csv(spark, path).count()

    recon = []
    for table in list(PARQUET_TABLES) + [Path(p).stem for p in files.get("reference", [])]:
        stats = (spark.read.parquet(str(settings.bronze_dir / table))
                 .groupBy("_source_file")
                 .agg(count("*").alias("n_rows"),
                      spark_sum(col("_source_sha256").isNull().cast("int")).alias("missing_lineage"))
                 .collect())
        for s in stats:
            raw_rows = expected.get(s["_source_file"])
            ok = raw_rows == s["n_rows"] and s["missing_lineage"] == 0
            recon.append(dict(table=table, file=s["_source_file"], raw_rows=raw_rows, bronze_rows=s["n_rows"],
                              missing_lineage=s["missing_lineage"], status="OK" if ok else "MISMATCH"))

    log.info("%-13s %-45s %12s %12s  status", "table", "file", "raw rows", "bronze rows")
    for r in sorted(recon, key=lambda r: (r["table"], r["file"])):
        log.info("%-13s %-45s %12s %12s  %s", r["table"], r["file"], f"{r['raw_rows'] or 0:,}",
                 f"{r['bronze_rows']:,}", r["status"])
    n_expected = sum(len(v) for v in files.values())
    passed = len(recon) == n_expected and all(r["status"] == "OK" for r in recon)
    return passed, recon, n_expected


def run(spark, settings):
    steps = Steps()
    files = ingestion.local_files(settings)
    if not files:
        raise QualityGateError(f"no raw file of the scope in {settings.raw_dir}: run the ingest layer first")
    with steps.step("schema check"):
        variants, schema_rows = check_schemas(spark, settings)
    with steps.step("Bronze tables"):
        build_tables(spark, settings, files)
    with steps.step("reconciliation"):
        passed, recon, n_expected = reconcile(spark, settings, files)
    total = sum(r["bronze_rows"] for r in recon)
    size = parquet_gb(settings.bronze_dir)
    log.info("Files reconciled: %d/%d | rows in Bronze: %s | Bronze size: %.2f GB | free space left: %.1f GB",
             len(recon), n_expected, f"{total:,}", size, free_gb(settings.data_root))
    log.info("Reconciliation: %s", "PASSED" if passed else "FAILED")
    report = write_report(settings, "bronze", dict(
        reconciliation_passed=passed, schema_variants=variants, files_reconciled=len(recon),
        files_expected=n_expected, rows=total, size_gb=round(size, 3), reconciliation=recon,
        durations_s=steps.durations))
    if not passed:
        raise QualityGateError("Bronze reconciliation failed: raw and Bronze row counts differ")
    return report
