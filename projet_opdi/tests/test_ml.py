"""Unit tests for the Machine Learning layer (GBTRegressor, Naive baseline, Scenarios, Airport Typology)."""
import datetime
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from opdi_pipeline import schemas
from opdi_pipeline.config import MLSettings, Settings
from opdi_pipeline.ml import (
    smooth_day,
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
    assert scenarios_df.filter("predicted_total_min_before < 0 AND saving_min_per_day IS NULL").count() == 0
    assert schemas.compare(scenarios_df.schema, schemas.GOLD["ml_scenarios"]) == []


def test_airport_typology_clustering(spark):
    def day(apt, arrivals, add, level, holding, measured_share=0.8):
        measured = int(arrivals * measured_share)
        return Row(airport_icao=apt, arrivals=arrivals, arrivals_measured=measured, avg_additional_min=add,
                   total_additional_min=add * measured, avg_level_minutes=level, pct_with_holding=holding)

    daily_rows = [
        day("LFPG", 300, 8.5, 7.2, 15.0), day("LFPG", 320, 9.0, 7.5, 18.0),
        day("LFBO", 120, 2.1, 2.0, 2.0), day("LFBO", 110, 1.9, 1.8, 1.5),
        day("ENZV", 40, 0.8, 0.5, 0.0), day("ENZV", 45, 0.9, 0.6, 0.0),
        day("EGLL", 400, 12.0, 9.0, 25.0), day("EGLL", 420, 13.0, 9.5, 28.0),
        day("LTFM", 500, 1.0, 1.0, 1.0, measured_share=0.01),   # arrival time almost never measured: excluded
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

    assert typology_df.count() == 4                                   # LTFM excluded (coverage 1 %)
    labels = {r["airport_icao"]: r["cluster_label"] for r in typology_df.collect()}
    assert "fluid" not in labels["EGLL"] and "fluid" in labels["ENZV"]
    assert meta["k"] in (2, 3)
    assert meta["silhouette"] >= -1.0
    # Schema check against contract
    assert schemas.compare(typology_df.schema, schemas.GOLD["airport_typology"]) == []


def test_demand_smoothing_moves_the_excess_to_the_nearest_hours():
    demand = [0] * 24
    demand[7], demand[8], demand[9] = 12, 10, 4
    new, moves = smooth_day(demand, cap=10)
    assert sum(new) == sum(demand)                          # no flight lost or created
    assert max(new) == 10                                   # cap respected
    assert new[7] == 10 and new[6] == 2                     # 8h is full: 7h -> 6h (the nearest hour with room)
    assert moves == {(7, 6): 2}
    full, moves = smooth_day([20] * 24, cap=10)             # no room anywhere: nobody moves
    assert full == [20] * 24 and moves == {}
