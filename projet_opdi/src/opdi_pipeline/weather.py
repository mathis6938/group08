"""Weather module (METAR integration) for arrival efficiency analysis.

Dedicated add-on module: enrich arrivals with hourly METAR weather observations
without modifying existing Bronze, Silver, Gold, or ML pipeline code.

Medallion Flow:
  - Ingestion: Download / ingest METAR observations from IEM ASOS archive into raw/weather.
  - Silver: Parse wind, visibility, cloud ceiling, IMC (Instrument Meteorological Conditions)
            and aggregate to hourly observations per airport (silver/weather_hourly).
  - Gold: LEFT JOIN silver/arrivals with silver/weather_hourly on (dest_icao, arrival_date, arrival_hour)
          to create gold/fact_arrivals_weather and gold/airport_weather_impact.
  - ML: Train and evaluate GBTRegressor with and without weather features to quantify
        variance explained by meteorology.
"""
import datetime
import json
import logging
import math
import os
import urllib.request
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .common import Steps, log, utc_now, write_table
from .config import Settings
from .ml import FOCUS_AIRPORTS, add_load_factor, build_arrival_ml_features, split_temporal

# Public IEM ASOS/METAR download endpoint (open access, no API key required)
IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# METAR CSV raw schema for IEM downloads
METAR_RAW_SCHEMA = StructType([
    StructField("station", StringType(), True),
    StructField("valid", StringType(), True),
    StructField("tmpc", StringType(), True),
    StructField("dwpc", StringType(), True),
    StructField("relh", StringType(), True),
    StructField("drct", StringType(), True),
    StructField("sknt", StringType(), True),
    StructField("gust", StringType(), True),
    StructField("vsby", StringType(), True),
    StructField("skyc1", StringType(), True),
    StructField("skyl1", StringType(), True),
    StructField("wxcodes", StringType(), True),
    StructField("metar", StringType(), True),
])


# ==============================================================================
# 1. Ingestion: Download METAR observations
# ==============================================================================

def build_iem_download_url(station: str, start_date: datetime.date, end_date: datetime.date) -> str:
    """Build URL to download METAR observations for one station from IEM ASOS archive."""
    params = [
        f"station={station}",
        "data=tmpc", "data=dwpc", "data=relh", "data=drct", "data=sknt",
        "data=gust", "data=vsby", "data=skyc1", "data=skyl1", "data=wxcodes", "data=metar",
        f"year1={start_date.year}", f"month1={start_date.month}", f"day1={start_date.day}",
        f"year2={end_date.year}", f"month2={end_date.month}", f"day2={end_date.day}",
        "tz=Etc%2FUTC",
        "format=onlycomma",
        "latlon=no",
        "missing=M",
        "trace=T",
        "direct=no",
        "report_type=3",
    ]
    return f"{IEM_ASOS_URL}?{'&'.join(params)}"


def download_metar_csv(station: str, start_date: datetime.date, end_date: datetime.date,
                       dest_path: Path, timeout_s: int = 30) -> bool:
    """Download METAR data from IEM for one airport and save as CSV."""
    url = build_iem_download_url(station, start_date, end_date)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading METAR for %s: %s -> %s", station, start_date, end_date)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "DAT535-Group08-OPDI/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as response:
            content = response.read().decode("utf-8", errors="replace")
            # Verify we got tabular data and not an empty error response
            lines = [l for l in content.splitlines() if l.strip() and not l.startswith("#")]
            if len(lines) <= 1:
                log.warning("No observations returned by IEM for %s", station)
                return False
            dest_path.write_text("\n".join(lines), encoding="utf-8")
            log.info("Saved %d METAR records for %s to %s", len(lines) - 1, station, dest_path)
            return True
    except Exception as exc:
        log.warning("Failed to download METAR for %s: %s", station, exc)
        return False


def ingest_weather_raw(settings: Settings, stations: list[str] = None) -> list[Path]:
    """Download METAR records for focus stations into raw/weather/<station>.csv."""
    stations = stations or FOCUS_AIRPORTS
    weather_raw_dir = settings.raw_dir / "weather"
    weather_raw_dir.mkdir(parents=True, exist_ok=True)
    saved_files = []
    for station in stations:
        csv_file = weather_raw_dir / f"{station}_metar.csv"
        # Download if file doesn't exist or is empty
        if not csv_file.exists() or csv_file.stat().st_size == 0:
            success = download_metar_csv(station, settings.start, settings.end, csv_file)
            if success:
                saved_files.append(csv_file)
        else:
            log.info("Using cached raw METAR for %s: %s", station, csv_file)
            saved_files.append(csv_file)
    return saved_files


# ==============================================================================
# 2. Silver: Parse, clean and aggregate hourly METAR observations
# ==============================================================================

def clean_metar_df(raw_df):
    """Parse raw METAR tabular strings into typed columns with aviation weather indicators."""
    # Cast and clean missing markers ('M', 'null', spaces)
    def to_float(col_name):
        return (F.when(F.col(col_name).isin("M", "null", "", "NA"), F.lit(None))
                .otherwise(F.col(col_name).cast(DoubleType())))

    cleaned = (
        raw_df
        .withColumn("station_icao", F.upper(F.trim(F.col("station"))))
        .withColumn("obs_time_utc", F.to_timestamp(F.col("valid"), "yyyy-MM-dd HH:mm"))
        .filter(F.col("obs_time_utc").isNotNull() & F.col("station_icao").isNotNull())
        .withColumn("obs_date", F.to_date(F.col("obs_time_utc")))
        .withColumn("obs_hour", F.hour(F.col("obs_time_utc")))
        .withColumn("temp_c", to_float("tmpc"))
        .withColumn("wind_dir_deg", to_float("drct"))
        .withColumn("wind_speed_kt", F.coalesce(to_float("sknt"), F.lit(0.0)))
        .withColumn("wind_gust_kt", F.coalesce(to_float("gust"), F.col("wind_speed_kt")))
        # Visibility in statute miles -> converted to meters (1 statute mile = 1609.34 m)
        .withColumn("visibility_sm", to_float("vsby"))
        .withColumn("visibility_m", F.coalesce(F.col("visibility_sm") * 1609.34, F.lit(10000.0)))
        # Cloud ceiling: lowest broken (BKN) or overcast (OVC) layer in feet
        .withColumn("ceiling_raw_ft", to_float("skyl1"))
        .withColumn("cloud_coverage", F.upper(F.trim(F.col("skyc1"))))
        .withColumn(
            "cloud_ceiling_ft",
            F.when(F.col("cloud_coverage").isin("BKN", "OVC"), F.coalesce(F.col("ceiling_raw_ft"), F.lit(10000.0)))
             .otherwise(F.lit(10000.0)),
        )
        # Aviation condition flags:
        # IMC (Instrument Meteorological Conditions): Visibility < 5000m OR ceiling < 1000 ft
        # This triggers Low Visibility Procedures (LVP), cutting runway throughput by up to 50%
        .withColumn(
            "is_imc",
            (F.col("visibility_m") < 5000.0) | (F.col("cloud_ceiling_ft") < 1000.0),
        )
        # Thunderstorms (TS)
        .withColumn("wxcodes_upper", F.coalesce(F.upper(F.col("wxcodes")), F.lit("")))
        .withColumn("metar_upper", F.coalesce(F.upper(F.col("metar")), F.lit("")))
        .withColumn(
            "has_thunderstorm",
            F.col("wxcodes_upper").contains("TS") | F.col("metar_upper").contains("TS"),
        )
        # Precipitation (Rain, Snow, Drizzle)
        .withColumn(
            "has_precipitation",
            F.col("wxcodes_upper").rlike("RA|SN|DZ|PL|GR|GS"),
        )
        # Fog / Mist
        .withColumn(
            "has_fog",
            F.col("wxcodes_upper").rlike("FG|BR|HZ|MIFG|BCFG"),
        )
    )
    return cleaned


def aggregate_weather_hourly(cleaned_df):
    """Aggregate cleaned sub-hourly observations to 1 representative row per (airport, date, hour)."""
    # Use max/worst weather condition of the hour for safety and delay explanation
    hourly_df = (
        cleaned_df
        .groupBy("station_icao", "obs_date", "obs_hour")
        .agg(
            F.round(F.avg("temp_c"), 1).alias("temp_c"),
            F.round(F.avg("wind_speed_kt"), 1).alias("wind_speed_kt"),
            F.round(F.max("wind_gust_kt"), 1).alias("wind_gust_kt"),
            F.round(F.avg("wind_dir_deg"), 0).alias("wind_dir_deg"),
            F.round(F.min("visibility_m"), 0).alias("visibility_m"),
            F.round(F.min("cloud_ceiling_ft"), 0).alias("cloud_ceiling_ft"),
            # An hour is IMC if any observation during that hour had LVP/IMC conditions
            F.max(F.col("is_imc").cast(IntegerType())).cast(BooleanType()).alias("is_imc"),
            F.max(F.col("has_thunderstorm").cast(IntegerType())).cast(BooleanType()).alias("has_thunderstorm"),
            F.max(F.col("has_precipitation").cast(IntegerType())).cast(BooleanType()).alias("has_precipitation"),
            F.max(F.col("has_fog").cast(IntegerType())).cast(BooleanType()).alias("has_fog"),
            F.count("*").alias("obs_count_in_hour"),
        )
    )
    return hourly_df


def build_silver_weather(spark, settings: Settings, raw_csv_paths: list[Path] = None):
    """Parse raw METAR files and write silver/weather_hourly table."""
    weather_raw_dir = settings.raw_dir / "weather"
    silver_weather_dir = settings.silver_dir / "weather_hourly"

    paths_to_read = raw_csv_paths or list(weather_raw_dir.glob("*_metar.csv"))
    if not paths_to_read:
        log.warning("No raw METAR CSV files found in %s", weather_raw_dir)
        return None

    str_paths = [str(p) for p in paths_to_read if p.stat().st_size > 0]
    if not str_paths:
        log.warning("All raw METAR files are empty")
        return None

    log.info("Reading %d raw METAR CSV files...", len(str_paths))
    raw_df = spark.read.option("header", "true").schema(METAR_RAW_SCHEMA).csv(str_paths)
    cleaned = clean_metar_df(raw_df)
    hourly = aggregate_weather_hourly(cleaned)

    write_table(hourly, silver_weather_dir, partition_cols=["station_icao"])
    log.info("Wrote silver/weather_hourly: %s", silver_weather_dir)
    return hourly


# ==============================================================================
# 3. Gold: Enrich arrivals with hourly METAR weather observations
# ==============================================================================

def enrich_arrivals_with_weather(spark, settings: Settings):
    """LEFT JOIN silver/arrivals with silver/weather_hourly to create gold/fact_arrivals_weather."""
    silver_arrivals_path = settings.silver_dir / "arrivals"
    silver_weather_path = settings.silver_dir / "weather_hourly"
    gold_weather_arrivals_path = settings.gold_dir / "fact_arrivals_weather"
    gold_weather_impact_path = settings.gold_dir / "airport_weather_impact"

    if not silver_arrivals_path.exists():
        raise FileNotFoundError(f"Missing silver/arrivals: {silver_arrivals_path}")
    if not silver_weather_path.exists():
        raise FileNotFoundError(f"Missing silver/weather_hourly: {silver_weather_path}")

    arrivals_df = spark.read.parquet(str(silver_arrivals_path))
    weather_df = spark.read.parquet(str(silver_weather_path))

    # Match arrival to the destination airport's weather observation of that exact arrival hour
    enriched = (
        arrivals_df
        .join(
            weather_df,
            (F.col("dest_icao") == F.col("station_icao")) &
            (F.col("arrival_date") == F.col("obs_date")) &
            (F.col("arrival_hour") == F.col("obs_hour")),
            "left",
        )
        .withColumn("has_weather_data", F.col("station_icao").isNotNull())
        # Impute non-limiting baseline weather defaults when station weather is unavailable
        .withColumn("wind_speed_kt", F.coalesce(F.col("wind_speed_kt"), F.lit(0.0)))
        .withColumn("wind_gust_kt", F.coalesce(F.col("wind_gust_kt"), F.lit(0.0)))
        .withColumn("visibility_m", F.coalesce(F.col("visibility_m"), F.lit(10000.0)))
        .withColumn("cloud_ceiling_ft", F.coalesce(F.col("cloud_ceiling_ft"), F.lit(10000.0)))
        .withColumn("is_imc", F.coalesce(F.col("is_imc"), F.lit(False)))
        .withColumn("has_thunderstorm", F.coalesce(F.col("has_thunderstorm"), F.lit(False)))
        .withColumn("has_precipitation", F.coalesce(F.col("has_precipitation"), F.lit(False)))
        .withColumn("has_fog", F.coalesce(F.col("has_fog"), F.lit(False)))
        # Wind categories for operational aggregation
        .withColumn(
            "wind_category",
            F.when(F.col("wind_speed_kt") < 10.0, F.lit("CALM_OR_LIGHT (<10kt)"))
             .when(F.col("wind_speed_kt") < 20.0, F.lit("MODERATE (10-20kt)"))
             .when(F.col("wind_speed_kt") < 30.0, F.lit("STRONG (20-30kt)"))
             .otherwise(F.lit("SEVERE (>=30kt)")),
        )
    )

    write_table(enriched, gold_weather_arrivals_path, partition_cols=["arrival_date"])
    log.info("Wrote gold/fact_arrivals_weather: %s", gold_weather_arrivals_path)

    # Compute operational impact summary table: additional time and holding by weather condition
    impact_summary = (
        enriched
        .filter(F.col("has_weather_data") == True)
        .groupBy("dest_icao", "is_imc", "wind_category", "has_thunderstorm")
        .agg(
            F.count("*").alias("flights_count"),
            F.round(F.avg("additional_time_min"), 2).alias("avg_additional_time_min"),
            F.round(F.avg("level_flight_time_min"), 2).alias("avg_level_flight_min"),
            F.round(F.avg(F.col("holding_flag").cast(DoubleType())) * 100, 1).alias("holding_rate_pct"),
            F.round(F.avg("fuel_excess_kg"), 1).alias("avg_fuel_excess_kg"),
        )
        .orderBy("dest_icao", F.desc("avg_additional_time_min"))
    )

    write_table(impact_summary, gold_weather_impact_path)
    log.info("Wrote gold/airport_weather_impact: %s", gold_weather_impact_path)

    return enriched, impact_summary


# ==============================================================================
# 4. ML: GBTRegressor enriched with METAR weather features
# ==============================================================================

def train_and_evaluate_weather_gbt(spark, enriched_arrivals_df, settings: Settings):
    """Train GBTRegressor with and without METAR weather features, comparing RMSE, MAE and R2."""
    # Filter to flights that have matching weather data and valid target
    df = enriched_arrivals_df.filter(
        (F.col("has_weather_data") == True) &
        F.col("additional_time_min").isNotNull() &
        (F.col("additional_time_min") >= -15.0) &
        (F.col("additional_time_min") <= 120.0)
    )

    if df.count() < 100:
        log.warning("Not enough arrivals with weather data to train ML model (%d rows)", df.count())
        return None

    # Harmonize column names and types
    prepped = (
        df
        .withColumn("day_of_week", F.dayofweek(F.col("landing_utc")).cast(DoubleType()))
        .withColumn("arrival_hour", F.col("arrival_hour").cast(DoubleType()))
        .withColumn("hourly_demand", F.coalesce(F.col("hourly_demand"), F.lit(10.0)).cast(DoubleType()))
        .withColumn("prev_hourly_demand", F.coalesce(F.col("prev_hourly_demand"), F.col("hourly_demand")).cast(DoubleType()))
        .withColumn("dest_icao", F.coalesce(F.col("dest_icao"), F.lit("UNKNOWN")))
        .withColumn("entry_sector", F.coalesce(F.col("entry_sector"), F.lit("UNKNOWN")))
        .withColumn("arrival_runway", F.coalesce(F.col("arrival_runway"), F.lit("UNKNOWN")))
        .withColumn("aircraft_class", F.coalesce(F.col("aircraft_class"), F.lit("UNKNOWN")))
        .withColumn("operator_clean", F.coalesce(F.col("operator_icao"), F.lit("OTHER")))
        # Weather numeric features
        .withColumn("wind_speed_kt", F.col("wind_speed_kt").cast(DoubleType()))
        .withColumn("wind_gust_kt", F.col("wind_gust_kt").cast(DoubleType()))
        .withColumn("visibility_km", (F.col("visibility_m") / 1000.0).cast(DoubleType()))
        .withColumn("ceiling_kft", (F.col("cloud_ceiling_ft") / 1000.0).cast(DoubleType()))
        .withColumn("is_imc_str", F.when(F.col("is_imc") == True, F.lit("IMC")).otherwise(F.lit("VMC")))
        .withColumn("has_ts_str", F.when(F.col("has_thunderstorm") == True, F.lit("TS")).otherwise(F.lit("NO_TS")))
        .withColumn("has_precip_str", F.when(F.col("has_precipitation") == True, F.lit("PRECIP")).otherwise(F.lit("NO_PRECIP")))
    )

    # Compute load factor
    airport_peak = (
        prepped.groupBy("dest_icao")
        .agg(F.percentile_approx("hourly_demand", 0.9, 1000).alias("airport_peak_demand"))
    )
    prepped = add_load_factor(prepped, airport_peak)

    # Temporal split
    train_df, test_df, split_date = split_temporal(prepped, settings.ml.test_days)

    # Baseline feature set (without weather - same as prod-8)
    base_cat = ["dest_icao", "entry_sector", "arrival_runway", "aircraft_class", "operator_clean"]
    base_num = ["arrival_hour", "day_of_week", "hourly_demand", "prev_hourly_demand", "load_factor"]

    # Enriched feature set (with weather)
    weather_cat = base_cat + ["is_imc_str", "has_ts_str", "has_precip_str"]
    weather_num = base_num + ["wind_speed_kt", "wind_gust_kt", "visibility_km", "ceiling_kft"]

    # String Indexers
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in weather_cat
    ]

    assembler_base = VectorAssembler(
        inputCols=[f"{c}_idx" for c in base_cat] + base_num,
        outputCol="features_base",
        handleInvalid="keep",
    )
    assembler_weather = VectorAssembler(
        inputCols=[f"{c}_idx" for c in weather_cat] + weather_num,
        outputCol="features_weather",
        handleInvalid="keep",
    )

    n_dest = prepped.select("dest_icao").distinct().count()
    max_bins = max(64, n_dest + 32, 512)

    gbt_base = GBTRegressor(
        featuresCol="features_base",
        labelCol="additional_time_min",
        predictionCol="pred_base",
        maxIter=settings.ml.max_iter,
        maxDepth=settings.ml.max_depth,
        maxBins=max_bins,
        seed=settings.ml.seed,
    )

    gbt_weather = GBTRegressor(
        featuresCol="features_weather",
        labelCol="additional_time_min",
        predictionCol="pred_weather",
        maxIter=settings.ml.max_iter,
        maxDepth=settings.ml.max_depth,
        maxBins=max_bins,
        seed=settings.ml.seed,
    )

    pipeline = Pipeline(stages=indexers + [assembler_base, assembler_weather, gbt_base, gbt_weather])
    log.info("Fitting GBT models (Base vs Weather-Enriched) on training set...")
    model = pipeline.fit(train_df)

    # Evaluate on test set
    predictions = model.transform(test_df)

    eval_rmse = RegressionEvaluator(labelCol="additional_time_min", metricName="rmse")
    eval_mae = RegressionEvaluator(labelCol="additional_time_min", metricName="mae")
    eval_r2 = RegressionEvaluator(labelCol="additional_time_min", metricName="r2")

    rmse_base = eval_rmse.evaluate(predictions.withColumn("prediction", F.col("pred_base")))
    mae_base = eval_mae.evaluate(predictions.withColumn("prediction", F.col("pred_base")))
    r2_base = eval_r2.evaluate(predictions.withColumn("prediction", F.col("pred_base")))

    rmse_weather = eval_rmse.evaluate(predictions.withColumn("prediction", F.col("pred_weather")))
    mae_weather = eval_mae.evaluate(predictions.withColumn("prediction", F.col("pred_weather")))
    r2_weather = eval_r2.evaluate(predictions.withColumn("prediction", F.col("pred_weather")))

    # Extract feature importances of the weather-enriched model
    gbt_weather_stage = model.stages[-1]
    importances = gbt_weather_stage.featureImportances.toArray().tolist()
    feature_names = [f"{c}_idx" for c in weather_cat] + weather_num
    importance_list = sorted(
        [{"variable": name, "importance": round(float(imp), 4)} for name, imp in zip(feature_names, importances)],
        key=lambda x: x["importance"],
        reverse=True,
    )

    comparison_results = {
        "split_date": str(split_date),
        "train_samples": train_df.count(),
        "test_samples": test_df.count(),
        "baseline_model": {
            "features_count": len(base_cat) + len(base_num),
            "mae_min": round(mae_base, 3),
            "rmse_min": round(rmse_base, 3),
            "r2": round(r2_base, 3),
        },
        "weather_enriched_model": {
            "features_count": len(weather_cat) + len(weather_num),
            "mae_min": round(mae_weather, 3),
            "rmse_min": round(rmse_weather, 3),
            "r2": round(r2_weather, 3),
        },
        "improvements": {
            "delta_mae_min": round(mae_base - mae_weather, 3),
            "delta_r2": round(r2_weather - r2_base, 3),
            "r2_improvement_pct": round(((r2_weather - r2_base) / max(0.001, abs(r2_base))) * 100, 1),
        },
        "feature_importances": importance_list,
    }

    log.info("=== ML Comparison Results ===")
    log.info("Base Model    : MAE=%.3f min, R2=%.3f", mae_base, r2_base)
    log.info("Weather Model : MAE=%.3f min, R2=%.3f (R2 gain: +%.1f%%)",
             mae_weather, r2_weather, comparison_results["improvements"]["r2_improvement_pct"])

    # Write results to Gold table
    report_path = settings.gold_dir / "ml_weather_comparison.json"
    report_path.write_text(json.dumps(comparison_results, indent=2))

    importance_rows = [
        (item["variable"], item["importance"], idx + 1)
        for idx, item in enumerate(importance_list)
    ]
    imp_schema = StructType([
        StructField("variable", StringType(), False),
        StructField("importance", DoubleType(), False),
        StructField("rank", IntegerType(), False),
    ])
    imp_df = spark.createDataFrame(importance_rows, imp_schema)
    write_table(imp_df, settings.gold_dir / "ml_weather_feature_importance")

    return comparison_results


# ==============================================================================
# 5. Pipeline Orchestrator
# ==============================================================================

def run_weather_pipeline(spark, settings: Settings, stations: list[str] = None):
    """Execute end-to-end weather module: Ingestion -> Silver -> Gold -> ML."""
    steps = Steps()
    stations = stations or FOCUS_AIRPORTS

    with steps.step("weather_ingestion"):
        raw_files = ingest_weather_raw(settings, stations)

    with steps.step("weather_silver"):
        silver_hourly = build_silver_weather(spark, settings, raw_files)

    with steps.step("weather_gold"):
        enriched_arrivals, impact_summary = enrich_arrivals_with_weather(spark, settings)

    with steps.step("weather_ml"):
        ml_results = train_and_evaluate_weather_gbt(spark, enriched_arrivals, settings)

    return {
        "status": "SUCCESS",
        "stations": stations,
        "steps_durations_s": steps.durations,
        "ml_results": ml_results,
    }
