"""Weather (METAR): parsing of the archive fields, weather conditions, quality rules, hourly Gold aggregation."""
import datetime

import pytest

from opdi_pipeline import gold, weather
from opdi_pipeline.config import load_settings

START, END = datetime.date(2026, 6, 9), datetime.date(2026, 6, 19)
FINE = dict(tmpc="18.00", dwpc="10.00", relh="60.00", drct="240.00", sknt="8.00", gust="M", vsby="6.21",
            skyc1="FEW", skyc2="M", skyc3="M", skyc4="M", skyl1="3500.00", skyl2="M", skyl3="M", skyl4="M",
            wxcodes="M", metar="LFBO 090000Z AUTO")


def bronze_rows(spark, rows):
    """Rows of bronze/metar (every column as text, with lineage) from dicts that change a fine-weather METAR."""
    full = [{"station": "LFBO", "valid": "2026-06-09 10:00", **FINE, **r, "_source_file": "metar_LFBO.csv",
             "_ingestion_run_id": "test", "_source_sha256": "abc"} for r in rows]
    return spark.createDataFrame(full, ", ".join(f"{c} string" for c in (*weather.METAR_COLUMNS, *weather.LINEAGE)))


def standardized(spark, rows):
    settings = load_settings("dev")
    return weather.standardize_metar(bronze_rows(spark, rows), settings.weather, START, END).collect()


def test_fields_are_typed_and_converted(spark):
    r = standardized(spark, [dict(vsby="6.21", sknt="12.00", gust="M", drct="VRB")])[0]
    assert r["visibility_m"] == pytest.approx(9994.0, abs=1)         # 6.21 statute miles = "9999" in the METAR
    assert r["wind_speed_kt"] == 12.0 and r["wind_gust_kt"] is None and r["wind_dir_deg"] is None
    assert r["obs_date"] == datetime.date(2026, 6, 9) and r["obs_hour"] == 10       # archive times in UTC
    assert r["ceiling_ft"] is None                                    # only a FEW layer: no ceiling
    assert r["present_weather"] is None and not (r["is_imc"] or r["has_precipitation"] or r["is_strong_wind"])


def test_ceiling_is_the_lowest_broken_or_overcast_layer(spark):
    rows = standardized(spark, [
        dict(skyc1="FEW", skyl1="500.00", skyc2="BKN", skyl2="1200.00", skyc3="OVC", skyl3="800.00"),
        dict(skyc1="VV", skyl1="100.00"),                               # vertical visibility (sky obscured)
        dict(skyc1="SCT", skyl1="900.00", skyc2="///", skyl2="M")])
    assert [r["ceiling_ft"] for r in rows] == [800.0, 100.0, None]
    assert [r["is_imc"] for r in rows] == [True, True, False]           # ceiling < 1 500 ft
    assert [r["is_below_cat1"] for r in rows] == [False, True, False]   # ceiling < 200 ft


@pytest.mark.parametrize("codes, ts, precip, fog", [
    ("+TSRA BCFG", True, True, True),
    ("-SHRA", False, True, False),
    ("VCSH", False, False, False),          # showers in the vicinity, not at the airport
    ("VCTS", False, False, False),          # thunderstorm in the vicinity
    ("BR", False, False, False),            # mist is not fog
    ("HZ", False, False, False),
    ("FZFG", False, False, True),
    ("-SHGR", False, True, False),
    ("TS", True, False, False),             # thunderstorm without precipitation
])
def test_present_weather_codes(spark, codes, ts, precip, fog):
    r = standardized(spark, [dict(wxcodes=codes)])[0]
    assert (r["has_thunderstorm"], r["has_precipitation"], r["has_fog"]) == (ts, precip, fog)


def test_trend_forecast_of_the_raw_metar_is_ignored(spark):
    r = standardized(spark, [dict(wxcodes="M", metar="LFPG 171200Z 24008KT 9999 FEW030 TEMPO TSRA")])[0]
    assert r["has_thunderstorm"] is False


def test_visibility_and_wind_thresholds(spark):
    rows = standardized(spark, [dict(vsby="3.00"),                    # 4 828 m: IMC, above CAT I
                                dict(vsby="0.31"),                    # 499 m: below CAT I
                                dict(sknt="20.00"), dict(sknt="12.00", gust="30.00"), dict(sknt="19.00", gust="29.00")])
    assert [r["is_imc"] for r in rows[:2]] == [True, True]
    assert [r["is_below_cat1"] for r in rows[:2]] == [False, True]
    assert [r["is_strong_wind"] for r in rows[2:]] == [True, True, False]


def test_quality_rules_and_scope(spark):
    settings = load_settings("dev")
    std = weather.standardize_metar(bronze_rows(spark, [
        dict(valid="not a date"), dict(sknt="-5.00"), dict(drct="400.00"), dict(vsby="-1.00"), dict(tmpc="75.00"),
        dict(station=" "), dict(valid="2026-06-19 00:00"), dict()]), settings.weather, START, END)
    assert std.filter("NOT _in_scope").count() == 1                    # June 19 = end of the scope (exclusive)
    reasons = [r["_quarantine_reason"] for r in weather.flag_metar(std.filter("_in_scope")).collect()]
    assert reasons == ["UNPARSABLE_TIMESTAMP", "INVALID_WIND", "INVALID_WIND", "INVALID_VISIBILITY",
                       "INVALID_TEMPERATURE", "MISSING_STATION", None]


def test_hourly_weather_and_category(spark):
    """Gold SQL: worst condition of the hour, vector mean of the wind direction, most severe category first."""
    settings = load_settings("dev")
    rows = [dict(valid="2026-06-10 15:00", drct="350.00"), dict(valid="2026-06-10 15:30", drct="10.00"),
            dict(valid="2026-06-10 16:00", wxcodes="-RA", sknt="22.00"), dict(valid="2026-06-10 16:30", gust="31.00"),
            dict(valid="2026-06-10 17:00", wxcodes="+TSRA", vsby="0.31"), dict(valid="2026-06-10 18:00")]
    obs = weather.standardize_metar(bronze_rows(spark, rows), settings.weather, START, END)
    obs.createOrReplaceTempView("silver_weather_observations")
    h = {r["weather_hour"]: r for r in spark.sql(gold.sql_text("airport_weather_hourly")).collect()}
    assert h[15]["observations"] == 2 and h[15]["wind_dir_deg"] == 0.0 and h[15]["weather_category"] == "GOOD"
    assert h[16]["max_gust_kt"] == 31.0 and h[16]["avg_wind_speed_kt"] == 15.0
    assert h[16]["weather_category"] == "STRONG_WIND"                  # strong wind before precipitation
    assert h[17]["weather_category"] == "THUNDERSTORM"                 # thunderstorm before low visibility
    assert h[17]["min_visibility_m"] == pytest.approx(499, abs=1)
    assert h[18]["weather_category"] == "GOOD"
