"""Silver cleaning and quality rules."""
import datetime

from pyspark.sql.functions import col

from opdi_pipeline.silver import clean_str, flag_events, flag_flights, icao_code, in_scope

T = datetime.datetime


def test_clean_str_gives_a_single_missing_value(spark):
    df = spark.createDataFrame([("  AFR123 ",), ("",), ("   ",), ("nan",), ("NaN",), (None,)], "v string")
    assert [r[0] for r in df.select(clean_str("v")).collect()] == ["AFR123", None, None, None, None, None]


def test_icao_code(spark):
    df = spark.createDataFrame([(" lfbo ",), ("ENZV",), ("LF",), ("",), ("nan",), (None,)], "v string")
    assert [r[0] for r in df.select(icao_code("v")).collect()] == ["LFBO", "ENZV", None, None, None, None]


def test_in_scope_keeps_unparsable_times_for_quarantine(spark):
    # times given as UTC strings and cast by Spark (session time zone UTC), as in the pipeline
    start, end = datetime.date(2026, 6, 1), datetime.date(2026, 7, 29)
    day = datetime.timedelta(days=1)
    rows = [(f"{start} 00:00:00",),                 # first instant: in
            (f"{end - day} 23:59:59",),             # last second: in
            (f"{end} 00:00:00",),                   # END is exclusive: out
            (f"{start - day} 23:59:59",),           # out
            (None,)]                                # unparsable: kept for the quality rules
    df = spark.createDataFrame(rows, "s string").select(col("s").cast("timestamp").alias("ts"))
    assert [r[0] for r in df.select(in_scope(col("ts"), start, end)).collect()] == [True, True, False, False, True]


FLIGHT_SCHEMA = ("flight_id long, transponder_icao24 string, origin_icao string, dest_icao string, "
                 "first_seen_utc timestamp, last_seen_utc timestamp, duration_min double")


def flight(fid, icao24="4ca123", adep="LFBO", ades="ENZV", start=T(2026, 6, 10, 8), minutes=120.0):
    end = None if start is None else start + datetime.timedelta(minutes=minutes)
    return (fid, icao24, adep, ades, start, end, None if start is None else minutes)


def reasons(df):
    return {r["flight_id"]: r["_quarantine_reason"] for r in df.collect()}


def test_flight_rules(spark):
    rows = [flight(1),                                   # valid
            flight(None),                                # no id
            flight(3, icao24=None),                      # no transponder address
            flight(4, start=None),                       # unparsable time
            flight(5, minutes=-30.0),                    # last_seen before first_seen
            flight(6, minutes=1500.0),                   # longer than 24 h
            flight(7, adep=None, ades=None, minutes=1.0)]   # valid, but flagged
    out = flag_flights(spark.createDataFrame(rows, FLIGHT_SCHEMA))
    r = reasons(out)
    assert r[1] is None and r[7] is None
    assert r[None] == "MISSING_FLIGHT_ID"
    assert r[3] == "MISSING_ICAO24"
    assert r[4] == "UNPARSABLE_TIMESTAMP"
    assert r[5] == "INVALID_CHRONOLOGY"
    assert r[6] == "DURATION_GT_24H"
    f7 = out.filter("flight_id = 7").first()
    assert (f7["has_adep"], f7["has_ades"], f7["is_short_flight"]) == (False, False, True)
    f1 = out.filter("flight_id = 1").first()
    assert (f1["has_adep"], f1["has_ades"], f1["is_short_flight"]) == (True, True, False)


EVENT_SCHEMA = ("event_id long, flight_id long, event_type string, event_timestamp_utc timestamp, "
                "latitude double, longitude double, altitude_ft double")


def test_event_rules(spark):
    t = T(2026, 6, 10, 9)
    rows = [(1, 10, "take-off", t, 43.6, 1.4, 500.0),        # valid
            (2, None, "take-off", t, 43.6, 1.4, 500.0),      # no flight id
            (3, 10, None, t, 43.6, 1.4, 500.0),              # no type
            (4, 10, "landing", None, 43.6, 1.4, 500.0),      # unparsable time
            (5, 10, "landing", t, 95.0, 1.4, 500.0),         # latitude out of range
            (6, 99, "landing", t, 43.6, 1.4, 500.0),         # flight unknown in Silver
            (7, 10, "level-start", t, None, None, 90000.0)]  # valid (no position), altitude flagged
    valid_ids = spark.createDataFrame([(10,), (10,), (11,)], "flight_id long")   # duplicates are allowed
    out = flag_events(spark.createDataFrame(rows, EVENT_SCHEMA), valid_ids)
    r = {row["event_id"]: row["_quarantine_reason"] for row in out.collect()}
    assert r == {1: None, 2: "MISSING_FLIGHT_ID", 3: "MISSING_EVENT_TYPE", 4: "UNPARSABLE_TIMESTAMP",
                 5: "INVALID_COORDINATES", 6: "ORPHAN_EVENT", 7: None}
    assert out.count() == len(rows)                          # the join does not duplicate events
    alt = {row["event_id"]: row["is_altitude_valid"] for row in out.collect()}
    assert alt[1] is True and alt[7] is False
