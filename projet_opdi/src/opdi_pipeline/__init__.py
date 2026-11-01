"""OPDI air-traffic pipeline (DAT535, group 08): Medallion architecture (Bronze -> Silver -> Gold) with PySpark.

Layers (one module each, run in this order):
    ingestion  download the OPDI and OurAirports files, manifest           (raw/)
    bronze     schema check, Bronze tables with row-level lineage, reconciliation   (bronze/)
    silver     standardisation, quality rules, quarantine, deduplication, enrichment (silver/, quarantine/)
    gold       airport traffic, runway usage, ground times, validation, publication (gold/)
    quality    end-to-end summary of the run and quality gates

Command line: python projet_opdi/run_pipeline.py --env dev --layer all
"""
# On the VM, Spark 3.5 lives in /opt/spark: findspark must point Python to it *before* any pyspark import
# (the pip pyspark of ~/spark-env is another version). This runs on the first `import opdi_pipeline`.
try:
    import findspark
except ImportError:          # tests / GitHub runner: PySpark comes from pip, nothing to find
    findspark = None
if findspark is not None:
    try:
        findspark.init()
    except (ValueError, IndexError) as e:      # no Spark installation found: use the pip PySpark
        import warnings
        warnings.warn(f"findspark could not locate Spark ({e}); using the installed pyspark package")

LAYERS = ("ingest", "bronze", "silver", "gold", "summary")
