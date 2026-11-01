"""Command line of the pipeline.

    python projet_opdi/run_pipeline.py --env dev --layer all              # the whole pipeline
    python projet_opdi/run_pipeline.py --env dev --layer silver           # one layer (inputs already on disk)
    python projet_opdi/run_pipeline.py --env prod --layer bronze,silver --start 2026-06-01 --end 2026-06-11
    python projet_opdi/run_pipeline.py --env prod --full-refresh           # rebuild all the Bronze periods
    python projet_opdi/run_pipeline.py --env dev --layer explore           # on demand: arrival-phase data checks

Exit code 1 if a layer fails (e.g. a quality gate): the next layers are not run, the summary is still written.
"""
import argparse
import logging
import sys
import time

from . import LAYERS, TOOLS
from .common import QualityGateError, log
from .config import load_settings


def parse_layers(value):
    known = LAYERS + TOOLS
    layers = list(LAYERS) if value == "all" else [v.strip() for v in value.split(",") if v.strip()]
    unknown = [v for v in layers if v not in known]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown layer(s) {unknown}; choose among {', '.join(known)} or all")
    return sorted(layers, key=known.index)                      # always in pipeline order


def run_layer(layer, spark, settings):
    from . import bronze, explore, gold, ingestion, quality, silver
    if layer == "ingest":
        return ingestion.run(settings)
    modules = {"bronze": bronze, "silver": silver, "gold": gold, "summary": quality, "explore": explore}
    return modules[layer].run(spark, settings)


def main(argv=None):
    parser = argparse.ArgumentParser(description="OPDI Medallion pipeline (Bronze -> Silver -> Gold)")
    parser.add_argument("--env", choices=["dev", "prod"], required=True, help="settings: config/<env>.toml")
    parser.add_argument("--layer", type=parse_layers, default=list(LAYERS),
                        help=f"all (default) or a comma-separated list among: {', '.join(LAYERS)}")
    parser.add_argument("--start", help="first day of the scope, YYYY-MM-DD (overrides the config)")
    parser.add_argument("--end", help="end of the scope, exclusive, YYYY-MM-DD (overrides the config)")
    parser.add_argument("--data-root", help="data folder (overrides the config)")
    parser.add_argument("--run-id", help="identifier shared by the layers of one run (default: OPDI_RUN_ID or time)")
    parser.add_argument("--full-refresh", action="store_true", default=None,
                        help="rebuild every Bronze period, even if its source file did not change")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", stream=sys.stdout)
    settings = load_settings(args.env, start=args.start, end=args.end, data_root=args.data_root, run_id=args.run_id,
                             full_refresh=args.full_refresh)
    log.info("OPDI pipeline | env=%s | layers=%s | scope [%s, %s[ | data %s | run %s%s", settings.env,
             ",".join(args.layer), settings.start, settings.end, settings.data_root, settings.run_id,
             " | full refresh" if settings.full_refresh else "")

    spark = None
    if any(layer != "ingest" for layer in args.layer):
        from .spark import get_spark
        spark = get_spark(settings)
        log.info("Spark %s", spark.version)

    status = 0
    for layer in args.layer:
        t0 = time.time()
        log.info("========== %s ==========", layer.upper())
        try:
            result = run_layer(layer, spark, settings)
        except QualityGateError as e:
            log.error("%s: QUALITY GATE FAILED: %s", layer, e)
            status = 1
        except Exception:
            log.exception("%s: FAILED", layer)
            status = 1
        else:
            if layer == "summary" and result["status"] != "PASSED":
                status = 1
            log.info("%s finished in %.0f s", layer, time.time() - t0)
        if status:
            break
    if status and "summary" in args.layer and layer != "summary":     # always leave a summary of the run
        log.info("========== SUMMARY (after failure) ==========")
        run_layer("summary", spark, settings)
    if spark is not None:
        spark.stop()
    return status


if __name__ == "__main__":
    sys.exit(main())
