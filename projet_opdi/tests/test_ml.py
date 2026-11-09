"""Unit tests for the Machine Learning layer (GBTRegressor, Naive baseline, Scenarios, Airport Typology)."""
import datetime
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from opdi_pipeline import schemas
from opdi_pipeline.config import MLSettings, Settings
from opdi_pipeline.ml import (
    build_airport_typology,
    build_arrival_ml_features,
    build_scenarios_table,
    split_temporal,
    train_and_evaluate_gbt,
)


@pytest.fixture(scope="module")
def spark():
    from opdi_pipeline.spark import get_spark
    from opdi_pipeline.config import load_settings
    s = load_settings("dev")
    spark = get_spark(s)
    yield spark


def test_split_temporal(spark):
    data = [
        Row(arrival_date=datetime.date(2026, 6, 9), val=1),
        Row(arrival_date=datetime.date(2026, 6, 10), val=2),
        Row(arrival_date=datetime.date(2026, 6, 11), val=3),
        Row(arrival_date=datetime.date(2026, 6, 12), val=4),
        Row(arrival_date=datetime.date(2026, 6, 13), val=5),
    ]
    df = spark.createDataFrame(data)
    train, test, split_date = split_temporal(df, test_days=2)

    assert train.count() > 0
    assert test.count() > 0
    assert train.filter(F.col("arrival_date") >= F.lit(split_date)).count() == 0
    assert test.filter(F.col("arrival_date") < F.lit(split_date)).count() == 0


def test_gbt_and_baseline_evaluation(spark):
    # Create small synthetic arrivals and ops for GBT training
    base_time = datetime.datetime(2026, 6, 10, 12, 0)
    rows = []
    for i in range(40):
        rows.append(Row(
            flight_id=i + 1,
            dest_icao="LFBO" if i % 2 == 0 else "LFPG",
            arrival_date=datetime.date(2026, 6, 10 + (i // 15)),
            arrival_hour=10 + (i % 4),
            day_of_week=float((i % 7) + 1),
            hourly_demand=float(5 + (i % 10)),
            prev_hourly_demand=float(4 + (i % 8)),
            entry_sector="N" if i % 2 == 0 else "S",
            arrival_runway="32L" if i % 2 == 0 else "09L",
            aircraft_class="L2J",
            operator_icao="AFR" if i % 3 == 0 else "EZY",
            level_off_count=float(i % 3),
            additional_min=float(2.0 + (i % 5) * 1.5),
        ))
    df = spark.createDataFrame(rows)
    train_df = df.filter("arrival_date < '2026-06-12'")
    test_df = df.filter("arrival_date >= '2026-06-12'")

    class DummyML:
        ml = MLSettings(max_iter=5, max_depth=3, seed=42)

    model, test_prepped, metrics = train_and_evaluate_gbt(train_df, test_df, DummyML())

    assert "model" in metrics
    assert "baseline" in metrics
    assert metrics["model"]["mae"] >= 0.0
    assert metrics["baseline"]["mae"] >= 0.0
    assert len(metrics["feature_importances"]) > 0
    assert metrics["feature_importances"][0]["rank"] == 1

    # Feature importance schema check
    fi_schema = "variable string, importance double, rank int, _ml_built_at_utc timestamp"
    fi_data = [{**r, "_ml_built_at_utc": datetime.datetime.now(datetime.timezone.utc)} for r in metrics["feature_importances"]]
    fi_df = spark.createDataFrame(fi_data, schema=fi_schema)
    assert schemas.compare(fi_df.schema, schemas.GOLD["ml_feature_importance"]) == []

    # Scenarios schema check
    scenarios_df = build_scenarios_table(spark, model, test_prepped, DummyML())
    assert scenarios_df.count() > 0
    assert schemas.compare(scenarios_df.schema, schemas.GOLD["ml_scenarios"]) == []


def test_airport_typology_clustering(spark):
    daily_rows = [
        Row(airport_icao="LFPG", arrivals=300, avg_additional_min=8.5, avg_level_minutes=7.2, pct_with_holding=15.0),
        Row(airport_icao="LFPG", arrivals=320, avg_additional_min=9.0, avg_level_minutes=7.5, pct_with_holding=18.0),
        Row(airport_icao="LFBO", arrivals=120, avg_additional_min=2.1, avg_level_minutes=2.0, pct_with_holding=2.0),
        Row(airport_icao="LFBO", arrivals=110, avg_additional_min=1.9, avg_level_minutes=1.8, pct_with_holding=1.5),
        Row(airport_icao="ENZV", arrivals=40, avg_additional_min=0.8, avg_level_minutes=0.5, pct_with_holding=0.0),
        Row(airport_icao="ENZV", arrivals=45, avg_additional_min=0.9, avg_level_minutes=0.6, pct_with_holding=0.0),
        Row(airport_icao="EGLL", arrivals=400, avg_additional_min=12.0, avg_level_minutes=9.0, pct_with_holding=25.0),
        Row(airport_icao="EGLL", arrivals=420, avg_additional_min=13.0, avg_level_minutes=9.5, pct_with_holding=28.0),
    ]
    daily_df = spark.createDataFrame(daily_rows)
    airports_dim = spark.createDataFrame([
        Row(airport_icao="LFPG", airport_name="Charles de Gaulle"),
        Row(airport_icao="LFBO", airport_name="Blagnac"),
        Row(airport_icao="ENZV", airport_name="Sola"),
        Row(airport_icao="EGLL", airport_name="Heathrow"),
    ])

    class DummyML:
        ml = MLSettings(min_arrivals=10, k_min=2, k_max=3, seed=42)

    typology_df, meta = build_airport_typology(spark, DummyML(), daily_df, airports_dim)

    assert typology_df.count() == 4
    assert meta["k"] in (2, 3)
    assert meta["silhouette"] >= -1.0
    # Schema check against contract
    assert schemas.compare(typology_df.schema, schemas.GOLD["airport_typology"]) == []
