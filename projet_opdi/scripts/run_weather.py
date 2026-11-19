#!/usr/bin/env python3
"""Run METAR weather enrichment pipeline standalone.

Usage:
    python scripts/run_weather.py --env dev
    python scripts/run_weather.py --env prod --stations EGLL LFPG EDDF
"""
import argparse
import sys
from pathlib import Path

# Add src to path
project_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_dir / "src"))

from opdi_pipeline.config import Settings, load_settings
from opdi_pipeline.spark import get_spark
from opdi_pipeline.weather import run_weather_pipeline


def main():
    parser = argparse.ArgumentParser(description="Run METAR weather enrichment pipeline")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Environment to run against")
    parser.add_argument("--stations", nargs="+", default=None, help="Airport ICAO codes (e.g. EGLL LFPG EDDF)")
    args = parser.parse_args()

    settings = load_settings(args.env)
    print(f"Starting Weather Pipeline in {args.env.upper()} mode...")
    print(f"Data root: {settings.data_root}")
    print(f"Scope: {settings.start} -> {settings.end}")

    spark = get_spark(settings)
    try:
        results = run_weather_pipeline(spark, settings, stations=args.stations)
        print("\nWeather Pipeline Finished Successfully!")
        print(f"Durations: {results['steps_durations_s']}")
        if results.get("ml_results"):
            ml = results["ml_results"]
            print("\nML Performance Summary:")
            print(f"  Base Model    : MAE={ml['baseline_model']['mae_min']} min, R2={ml['baseline_model']['r2']}")
            print(f"  Weather Model : MAE={ml['weather_enriched_model']['mae_min']} min, R2={ml['weather_enriched_model']['r2']}")
            print(f"  Improvement   : +{ml['improvements']['r2_improvement_pct']}% on R2")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
