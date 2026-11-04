"""Unit tests for arrival efficiency rules and pure geometry helpers."""
import datetime
import pytest
from pyspark.sql import functions as F

from opdi_pipeline.arrivals import (
    bearing_deg_py,
    bearing_to_sector_py,
    haversine_km_py,
    interpolate_ring_entry_py,
    spark_bearing_deg,
    spark_entry_sector,
    spark_haversine_km,
)


def test_bearing_deg_cardinal_directions():
    # From Paris (49.0, 2.5) to north
    assert bearing_deg_py(49.0, 2.5, 50.0, 2.5) == pytest.approx(0.0, abs=0.1)
    # To south
    assert bearing_deg_py(49.0, 2.5, 48.0, 2.5) == pytest.approx(180.0, abs=0.1)
    # To east
    b_east = bearing_deg_py(0.0, 0.0, 0.0, 1.0)
    assert b_east == pytest.approx(90.0, abs=0.1)
    # To west
    b_west = bearing_deg_py(0.0, 0.0, 0.0, -1.0)
    assert b_west == pytest.approx(270.0, abs=0.1)


def test_bearing_to_sector_all_8_sectors():
    assert bearing_to_sector_py(0.0) == "N"
    assert bearing_to_sector_py(15.0) == "N"
    assert bearing_to_sector_py(350.0) == "N"
    assert bearing_to_sector_py(45.0) == "NE"
    assert bearing_to_sector_py(90.0) == "E"
    assert bearing_to_sector_py(135.0) == "SE"
    assert bearing_to_sector_py(180.0) == "S"
    assert bearing_to_sector_py(225.0) == "SW"
    assert bearing_to_sector_py(270.0) == "W"
    assert bearing_to_sector_py(315.0) == "NW"


def test_bearing_to_sector_boundaries():
    # Boundary tests for 8-sector compass
    assert bearing_to_sector_py(22.4) == "N"
    assert bearing_to_sector_py(22.5) == "NE"
    assert bearing_to_sector_py(67.4) == "NE"
    assert bearing_to_sector_py(67.5) == "E"
    assert bearing_to_sector_py(337.4) == "NW"
    assert bearing_to_sector_py(337.5) == "N"


def test_haversine_km_known_distance():
    # Paris LFPG (49.0097, 2.5479) to Toulouse LFBO (43.6291, 1.3638) ~ 590 km
    d = haversine_km_py(49.0097, 2.5479, 43.6291, 1.3638)
    assert 580 < d < 610


def test_interpolate_ring_entry():
    # Point 1 at 200 km, Point 2 at 150 km, Ring at 185.2 km
    # alpha = (200 - 185.2) / (200 - 150) = 14.8 / 50 = 0.296
    t1, lat1, lon1, d1 = 1000.0, 49.5, 2.0, 200.0
    t2, lat2, lon2, d2 = 1100.0, 49.0, 2.5, 150.0
    t_interp, lat_interp, lon_interp = interpolate_ring_entry_py(t1, lat1, lon1, d1, t2, lat2, lon2, d2, ring_km=185.2)
    alpha = (200.0 - 185.2) / 50.0
    assert t_interp == pytest.approx(1000.0 + alpha * 100.0)
    assert lat_interp == pytest.approx(49.5 + alpha * (49.0 - 49.5))
    assert lon_interp == pytest.approx(2.0 + alpha * (2.5 - 2.0))


def test_spark_bearing_and_sector(spark):
    df = spark.createDataFrame([
        (0.0, 0.0, 1.0, 0.0),    # due north -> N
        (0.0, 0.0, 0.0, 1.0),    # due east -> E
        (0.0, 0.0, -1.0, 0.0),   # due south -> S
        (0.0, 0.0, 0.0, -1.0),   # due west -> W
    ], ["lat1", "lon1", "lat2", "lon2"])
    res = (
        df
        .withColumn("bearing", spark_bearing_deg("lat1", "lon1", "lat2", "lon2"))
        .withColumn("sector", spark_entry_sector(F.col("bearing")))
        .collect()
    )
    sectors = [r["sector"] for r in res]
    assert sectors == ["N", "E", "S", "W"]


def test_spark_haversine_matches_python(spark):
    df = spark.createDataFrame([
        (49.0097, 2.5479, 43.6291, 1.3638),
        (58.8767, 5.6378, 60.1939, 11.1004),
    ], ["lat1", "lon1", "lat2", "lon2"])
    spark_res = [r[0] for r in df.select(spark_haversine_km("lat1", "lon1", "lat2", "lon2")).collect()]
    py_res = [haversine_km_py(49.0097, 2.5479, 43.6291, 1.3638),
              haversine_km_py(58.8767, 5.6378, 60.1939, 11.1004)]
    for s, p in zip(spark_res, py_res):
        assert s == pytest.approx(p, abs=0.1)


def test_holding_detection_logic():
    # Holding: duration >= 4.0 min and distance_flown >= 1.5 * displacement
    # Case 1: 5 min holding with 40 km flown and 5 km displacement -> holding
    dur, flown, disp = 5.0, 40.0, 5.0
    assert (dur >= 4.0) and (flown >= 1.5 * disp)

    # Case 2: 5 min straight level with 40 km flown and 39 km displacement -> not holding
    dur, flown, disp = 5.0, 40.0, 39.0
    assert not ((dur >= 4.0) and (flown >= 1.5 * disp))

    # Case 3: 2 min tight turn with 15 km flown and 2 km displacement -> dur < 4 min -> not holding
    dur, flown, disp = 2.0, 15.0, 2.0
    assert not ((dur >= 4.0) and (flown >= 1.5 * disp))

