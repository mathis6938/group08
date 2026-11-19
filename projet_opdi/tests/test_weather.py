"""Unit tests for the METAR weather integration module."""
import datetime
import pytest
from pyspark.sql import Row
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

from opdi_pipeline.weather import (
    aggregate_weather_hourly,
    build_iem_download_url,
    clean_metar_df,
    METAR_RAW_SCHEMA,
)


def test_build_iem_download_url():
    """Verify download URL contains station, dates, and required data parameters."""
    start = datetime.date(2024, 6, 1)
    end = datetime.date(2024, 6, 15)
    url = build_iem_download_url("EGLL", start, end)

    assert "station=EGLL" in url
    assert "year1=2024" in url
    assert "month1=6" in url
    assert "day1=1" in url
    assert "year2=2024" in url
    assert "month2=6" in url
    assert "day2=15" in url
    assert "format=onlycomma" in url


def test_clean_metar_df(spark):
    """Test parsing of raw METAR string fields into typed columns and aviation indicators."""
    raw_data = [
        # Good VMC condition: clear, mild wind, no clouds
        Row(station="EGLL", valid="2024-06-10 12:20", tmpc="18.5", dwpc="10.0", relh="58.0",
            drct="240", sknt="12", gust="M", vsby="10.0", skyc1="CLR", skyl1="M",
            wxcodes="", metar="EGLL 101220Z 24012KT 9999 CLR 18/10 Q1015"),
        # Severe IMC condition: low visibility (0.5 SM ~ 800m), low ceiling (300 ft), fog
        Row(station="LFPG", valid="2024-06-10 06:00", tmpc="8.0", dwpc="7.8", relh="98.0",
            drct="020", sknt="5", gust="M", vsby="0.5", skyc1="OVC", skyl1="300",
            wxcodes="FG", metar="LFPG 100600Z 02005KT 0800 FG OVC003 08/08 Q1020"),
        # Thunderstorm with strong wind gusts
        Row(station="EDDF", valid="2024-06-10 16:50", tmpc="25.0", dwpc="18.0", relh="65.0",
            drct="270", sknt="25", gust="42", vsby="3.0", skyc1="BKN", skyl1="1500",
            wxcodes="TSRA", metar="EDDF 101650Z 27025G42KT 5000 +TSRA BKN015CB 25/18 Q1008"),
    ]
    raw_df = spark.createDataFrame(raw_data)
    cleaned = clean_metar_df(raw_df).collect()

    cleaned_by_station = {r["station_icao"]: r for r in cleaned}

    # EGLL checks (VMC)
    egll = cleaned_by_station["EGLL"]
    assert egll["wind_speed_kt"] == 12.0
    assert egll["wind_gust_kt"] == 12.0  # Coalesced from sknt when gust is missing
    assert egll["is_imc"] is False
    assert egll["has_thunderstorm"] is False
    assert egll["has_fog"] is False

    # LFPG checks (IMC / LVP due to fog and 300ft ceiling)
    lfpg = cleaned_by_station["LFPG"]
    assert lfpg["is_imc"] is True
    assert lfpg["has_fog"] is True
    assert lfpg["cloud_ceiling_ft"] == 300.0
    assert lfpg["visibility_m"] < 1000.0

    # EDDF checks (Thunderstorm + strong gusts)
    eddf = cleaned_by_station["EDDF"]
    assert eddf["wind_speed_kt"] == 25.0
    assert eddf["wind_gust_kt"] == 42.0
    assert eddf["has_thunderstorm"] is True
    assert eddf["has_precipitation"] is True


def test_aggregate_weather_hourly(spark):
    """Test that multiple observations within the same hour are aggregated into a single row."""
    # 3 observations during 14:00-14:59 hour at EGLL
    raw_data = [
        Row(station="EGLL", valid="2024-06-10 14:00", tmpc="20.0", dwpc="10.0", relh="50.0",
            drct="200", sknt="10", gust="M", vsby="10.0", skyc1="FEW", skyl1="2000",
            wxcodes="", metar=""),
        Row(station="EGLL", valid="2024-06-10 14:20", tmpc="19.0", dwpc="11.0", relh="55.0",
            drct="220", sknt="15", gust="22", vsby="6.0", skyc1="BKN", skyl1="800",
            wxcodes="-RA", metar=""),
        Row(station="EGLL", valid="2024-06-10 14:50", tmpc="18.0", dwpc="12.0", relh="60.0",
            drct="240", sknt="12", gust="M", vsby="8.0", skyc1="SCT", skyl1="1500",
            wxcodes="", metar=""),
    ]
    raw_df = spark.createDataFrame(raw_data)
    cleaned = clean_metar_df(raw_df)
    hourly = aggregate_weather_hourly(cleaned).collect()

    assert len(hourly) == 1
    h = hourly[0]
    assert h["station_icao"] == "EGLL"
    assert h["obs_hour"] == 14
    assert h["obs_count_in_hour"] == 3
    # Max gust across the hour is 22 kt
    assert h["wind_gust_kt"] == 22.0
    # IMC flag is True because the 14:20 observation had ceiling 800 ft (< 1000 ft)
    assert h["is_imc"] is True
    # Precipitation flag is True because 14:20 had -RA
    assert h["has_precipitation"] is True


def test_weather_enrichment_logic(spark):
    """Test the LEFT JOIN logic between arrivals and hourly weather observations."""
    # Synthetic arrivals
    arrivals_data = [
        Row(dest_icao="EGLL", arrival_date=datetime.date(2024, 6, 10), arrival_hour=14,
            transit_min=25.0, additional_time_min=5.5, holding_flag=False, fuel_excess_kg=70.0),
        Row(dest_icao="LFPG", arrival_date=datetime.date(2024, 6, 10), arrival_hour=6,
            transit_min=35.0, additional_time_min=12.0, holding_flag=True, fuel_excess_kg=150.0),
        # Airport without weather observation
        Row(dest_icao="LFBO", arrival_date=datetime.date(2024, 6, 10), arrival_hour=18,
            transit_min=18.0, additional_time_min=1.0, holding_flag=False, fuel_excess_kg=15.0),
    ]
    arrivals_df = spark.createDataFrame(arrivals_data)

    # Hourly weather table
    weather_data = [
        Row(station_icao="EGLL", obs_date=datetime.date(2024, 6, 10), obs_hour=14,
            wind_speed_kt=15.0, wind_gust_kt=22.0, visibility_m=9000.0, cloud_ceiling_ft=2500.0,
            is_imc=False, has_thunderstorm=False, has_precipitation=True, has_fog=False),
        Row(station_icao="LFPG", obs_date=datetime.date(2024, 6, 10), obs_hour=6,
            wind_speed_kt=6.0, wind_gust_kt=6.0, visibility_m=800.0, cloud_ceiling_ft=300.0,
            is_imc=True, has_thunderstorm=False, has_precipitation=False, has_fog=True),
    ]
    weather_df = spark.createDataFrame(weather_data)

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
        .withColumn("is_imc", F.coalesce(F.col("is_imc"), F.lit(False)))
    ).collect()

    by_dest = {r["dest_icao"]: r for r in enriched}

    # EGLL matched VMC
    assert by_dest["EGLL"]["has_weather_data"] is True
    assert by_dest["EGLL"]["is_imc"] is False
    assert by_dest["EGLL"]["wind_speed_kt"] == 15.0

    # LFPG matched IMC
    assert by_dest["LFPG"]["has_weather_data"] is True
    assert by_dest["LFPG"]["is_imc"] is True
    assert by_dest["LFPG"]["visibility_m"] == 800.0

    # LFBO gracefully handled without weather (coalesced default)
    assert by_dest["LFBO"]["has_weather_data"] is False
    assert by_dest["LFBO"]["is_imc"] is False

