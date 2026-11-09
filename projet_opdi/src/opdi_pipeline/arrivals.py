"""Arrival efficiency analysis (Silver layer): ASMA-inspired transit time & descent level-offs.

Two indicators:
  1. Indicator A (Arrival transit time): 100 NM ring entry interpolation to runway landing.
  2. Indicator B (Descent level-offs & holdings): level segments after descent start (<= 200 NM),
     chunk fusion, ground level-off exclusion, altitude bands, holding detection, and fuel/CO2 estimates.

Writes:
  - silver/arrivals
  - silver/descent_level_offs
"""
import math

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql.functions import (
    abs as spark_abs,
    asin, atan2, broadcast, coalesce, col,
    cos, current_timestamp, degrees, desc, hour,
    lag, lead, lit, max as spark_max, min as spark_min, struct,
    percentile_approx, radians, regexp_replace, row_number, sin, sqrt,
    sum as spark_sum, to_date, to_timestamp, trim, unix_timestamp, upper, when,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import DoubleType, LongType

from .common import Steps, log, write_table

FOCUS_AIRPORTS = ["LFPG", "LFBO", "ENZV", "EGLL", "EDDF"]


# --------------------------------------------------------------------------------- Pure geometry & physics helpers
def bearing_deg_py(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing in degrees [0, 360) from (lat1, lon1) to (lat2, lon2)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def bearing_to_sector_py(bearing: float) -> str:
    """Map bearing in [0, 360) to 8 standard 45-degree sectors."""
    b = bearing % 360.0
    if b >= 337.5 or b < 22.5:
        return "N"
    elif b < 67.5:
        return "NE"
    elif b < 112.5:
        return "E"
    elif b < 157.5:
        return "SE"
    elif b < 202.5:
        return "S"
    elif b < 247.5:
        return "SW"
    elif b < 292.5:
        return "W"
    else:
        return "NW"


def haversine_km_py(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (haversine formula, Earth radius = 6371 km)."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(a))


def interpolate_ring_entry_py(t1_s: float, lat1: float, lon1: float, d1_km: float,
                              t2_s: float, lat2: float, lon2: float, d2_km: float,
                              ring_km: float = 185.2):
    """Linear interpolation of timestamp and coordinates at ring_km boundary."""
    if d1_km == d2_km:
        return t1_s, lat1, lon1
    alpha = (d1_km - ring_km) / (d1_km - d2_km)
    t_interp = t1_s + alpha * (t2_s - t1_s)
    lat_interp = lat1 + alpha * (lat2 - lat1)
    lon_interp = lon1 + alpha * (lon2 - lon1)
    return t_interp, lat_interp, lon_interp


def _as_col(c):
    return col(c) if isinstance(c, str) else c


# --------------------------------------------------------------------------------- Spark SQL Expressions
def spark_haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two positions (Spark columns or column names)."""
    lat1, lon1, lat2, lon2 = _as_col(lat1), _as_col(lon1), _as_col(lat2), _as_col(lon2)
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2.0) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2.0) ** 2
    return lit(2.0 * 6371.0) * asin(sqrt(a))


def spark_bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing in degrees [0, 360) from airport (lat1, lon1) to entry point (lat2, lon2)."""
    lat1, lon1, lat2, lon2 = _as_col(lat1), _as_col(lon1), _as_col(lat2), _as_col(lon2)
    phi1, phi2 = radians(lat1), radians(lat2)
    dlambda = radians(lon2 - lon1)
    y = sin(dlambda) * cos(phi2)
    x = cos(phi1) * sin(phi2) - sin(phi1) * cos(phi2) * cos(dlambda)
    rad = atan2(y, x)
    deg = (degrees(rad) + 360.0) % 360.0
    return deg


def spark_entry_sector(bearing_col):
    """Map bearing degrees to 8 compass sectors."""
    return (
        when((bearing_col >= 337.5) | (bearing_col < 22.5), "N")
        .when((bearing_col >= 22.5) & (bearing_col < 67.5), "NE")
        .when((bearing_col >= 67.5) & (bearing_col < 112.5), "E")
        .when((bearing_col >= 112.5) & (bearing_col < 157.5), "SE")
        .when((bearing_col >= 157.5) & (bearing_col < 202.5), "S")
        .when((bearing_col >= 202.5) & (bearing_col < 247.5), "SW")
        .when((bearing_col >= 247.5) & (bearing_col < 292.5), "W")
        .otherwise("NW")
    )


# --------------------------------------------------------------------------------- Pipeline logic
def compute_level_offs(spark, settings, silver_flights, silver_events, silver_airports, bronze_measurements):
    """First half of the arrival study: the fused descent level-offs, and the per-flight parts of silver/arrivals.

    The inputs must be the *written* Silver tables (and Bronze measurements), never the lazy DataFrames of the Silver
    run: with lazy inputs Spark recomputes the whole Silver chain (Bronze read, standardisation, deduplication of
    ~86 M events) for each action, which exhausted the driver memory in prod (run prod-5, GC overhead limit).

    Returns (level_offs, parts): the level-offs DataFrame (to be written first) and the DataFrames that
    assemble_arrivals needs. parts["cached"] lists the persisted DataFrames to release once both tables are written.
    """
    cfg = settings.arrivals
    ring_radius_km = cfg.ring_radius_nm * 1.852
    descent_radius_km = cfg.descent_radius_nm * 1.852
    # The ring transition needs the event *before* the 100 NM ring: keep the events up to 3 x 100 NM (and at least
    # the 200 NM descent window). Everything farther is cruise, useless for both indicators.
    candidate_radius_km = max(3.0 * ring_radius_km, descent_radius_km)

    # 1. Target flights: destination known with coordinates
    airports_dim = silver_airports.select(
        col("airport_icao").alias("dest_icao"),
        coalesce(col("elevation_ft"), lit(0.0)).alias("dest_elevation_ft"),
    )
    dest_flights = (
        silver_flights
        .filter(col("dest_icao").isNotNull() & col("dest_lat").isNotNull() & col("dest_lon").isNotNull())
        .join(broadcast(airports_dim), "dest_icao", "left")
        .withColumn("dest_elevation_ft", coalesce(col("dest_elevation_ft"), lit(0.0)))
        .select(
            "flight_id", "dest_icao", "origin_icao", "operator_icao", "aircraft_typecode", "aircraft_class",
            "dest_lat", "dest_lon", "dest_elevation_ft", "first_seen_utc", "last_seen_utc",
            "_source_file", "_source_period", "_ingestion_run_id", "_source_sha256",
        )
    )

    # 2. Attach destination position to events of destination flights
    # Only the columns used below are read (Parquet column pruning: the `info` map is not read for all events), and
    # only the events near the destination are kept. The result is reused by several steps: persisted on disk.
    events_with_dest = (
        silver_events
        .select("event_id", "flight_id", "event_type", "event_timestamp_utc", "latitude", "longitude", "altitude_ft")
        .filter(col("latitude").between(-90, 90) & col("longitude").between(-180, 180))
        .join(
            dest_flights.select("flight_id", "dest_icao", "dest_lat", "dest_lon", "dest_elevation_ft", "last_seen_utc"),
            "flight_id",
            "inner",
        )
        .withColumn("dist_km", spark_haversine_km(col("latitude"), col("longitude"), col("dest_lat"), col("dest_lon")))
        .filter(col("dist_km") <= candidate_radius_km)
        .persist(StorageLevel.DISK_ONLY)
    )

    # 3. Indicator A: Landing detection (first entry-runway at destination <= elevation + 1500 ft)
    # Filter runway events at destination within ring radius
    # Separate scan of the runway events only (~2 % of the events): the only place where the `info` map is read.
    runway_events = (
        silver_events
        .filter(col("event_type") == "entry-runway")
        .select("event_id", "flight_id", "event_timestamp_utc", "latitude", "longitude", "altitude_ft",
                col("info").getItem("osm_airport").alias("osm_airport"),
                col("info").getItem("osm_ref").alias("osm_ref"))
        .join(dest_flights.select("flight_id", "dest_icao", "dest_lat", "dest_lon", "dest_elevation_ft"), "flight_id")
        .withColumn("dist_km", spark_haversine_km(col("latitude"), col("longitude"), col("dest_lat"), col("dest_lon")))
        .filter(
            (col("osm_airport") == col("dest_icao"))
            & (col("dist_km") <= ring_radius_km)
            & (col("altitude_ft") <= col("dest_elevation_ft") + 1500.0)
        )
    )
    w_landing = Window.partitionBy("flight_id").orderBy("event_timestamp_utc", "event_id")
    flight_landings = (
        runway_events
        .withColumn("_rn", row_number().over(w_landing))
        .filter(col("_rn") == 1)
        .select(
            "flight_id",
            col("event_timestamp_utc").alias("landing_utc"),
            regexp_replace(upper(trim(col("osm_ref"))), r"\s*[/;]\s*|\s+", "-").alias("arrival_runway"),
        )
    )

    # 4. Indicator A: 100 NM Ring Entry (last crossing from > 100 NM to <= 100 NM before landing)
    events_for_ring = (
        events_with_dest
        .join(flight_landings.select("flight_id", "landing_utc"), "flight_id", "left")
        .filter(col("landing_utc").isNull() | (col("event_timestamp_utc") < col("landing_utc")))
    )
    w_flight_events = Window.partitionBy("flight_id").orderBy("event_timestamp_utc", "event_id")
    transitions = (
        events_for_ring
        .withColumn("prev_dist_km", lag("dist_km").over(w_flight_events))
        .withColumn("prev_ts", lag("event_timestamp_utc").over(w_flight_events))
        .withColumn("prev_lat", lag("latitude").over(w_flight_events))
        .withColumn("prev_lon", lag("longitude").over(w_flight_events))
        .filter((col("prev_dist_km") > ring_radius_km) & (col("dist_km") <= ring_radius_km))
        .withColumn(
            "alpha",
            (col("prev_dist_km") - lit(ring_radius_km)) / (col("prev_dist_km") - col("dist_km")),
        )
        .withColumn(
            "ring_entry_unix",
            unix_timestamp("prev_ts") + col("alpha") * (unix_timestamp("event_timestamp_utc") - unix_timestamp("prev_ts")),
        )
        .withColumn("ring_entry_utc", to_timestamp("ring_entry_unix"))
        .withColumn("ring_entry_lat", col("prev_lat") + col("alpha") * (col("latitude") - col("prev_lat")))
        .withColumn("ring_entry_lon", col("prev_lon") + col("alpha") * (col("longitude") - col("prev_lon")))
        .withColumn("bearing_deg", spark_bearing_deg(col("dest_lat"), col("dest_lon"), col("ring_entry_lat"), col("ring_entry_lon")))
        .withColumn("entry_sector", spark_entry_sector(col("bearing_deg")))
    )
    # Pick the LAST transition before landing
    w_last_trans = Window.partitionBy("flight_id").orderBy(desc("ring_entry_utc"))
    flight_ring_entries = (
        transitions
        .withColumn("_rn", row_number().over(w_last_trans))
        .filter(col("_rn") == 1)
        .select(
            "flight_id", "ring_entry_utc",
            spark_round("ring_entry_lat", 4).alias("ring_entry_lat"),
            spark_round("ring_entry_lon", 4).alias("ring_entry_lon"),
            "entry_sector",
        )
    )

    # 5. Indicator B: Descent window determination
    # Events within 200 NM (370.4 km) from destination
    events_in_descent_radius = (
        events_with_dest
        .filter(col("dist_km") <= descent_radius_km)
        .join(flight_landings.select("flight_id", "landing_utc"), "flight_id", "left")
    )
    w_flight = Window.partitionBy("flight_id")
    # Highest altitude reached in this 200 NM window
    descent_bounds = (
        events_in_descent_radius
        .withColumn("max_alt_in_window", spark_max("altitude_ft").over(w_flight))
        # Last event at the highest altitude level reached in window (tolerance 300 ft)
        .withColumn("is_highest_level", col("altitude_ft") >= (col("max_alt_in_window") - 300.0))
        .withColumn("descent_start_utc", spark_max(when(col("is_highest_level"), col("event_timestamp_utc"))).over(w_flight))
        # Descent end: landing_utc if available, else last event at > elevation + 1000 ft, else last_seen_utc
        .withColumn("is_above_clearance", col("altitude_ft") >= (col("dest_elevation_ft") + cfg.min_ground_clearance_ft))
        .withColumn("last_event_above_clearance", spark_max(when(col("is_above_clearance"), col("event_timestamp_utc"))).over(w_flight))
        .withColumn("descent_end_utc", coalesce(col("landing_utc"), col("last_event_above_clearance"), col("last_seen_utc")))
        .select("flight_id", "descent_start_utc", "descent_end_utc")
        .distinct()
    )

    # 6. Indicator B: Level-off segments in descent window
    # Only the cumulative distance flown is used (holding detection): one measurement type, one row per event, so a
    # plain join on event_id (no aggregation of the ~119 M Bronze measurement rows).
    meas = (
        bronze_measurements
        .filter(col("type") == "Distance flown (NM)")
        .select(col("event_id").cast(LongType()).alias("event_id"),
                col("value").cast(DoubleType()).alias("distance_flown_nm"))
    )

    level_events = (
        events_with_dest
        .filter(col("event_type").isin("level-start", "level-end"))
        .join(descent_bounds, "flight_id", "inner")
        .filter(
            (col("event_timestamp_utc") >= col("descent_start_utc"))
            & (col("event_timestamp_utc") <= col("descent_end_utc"))
        )
        .join(meas, "event_id", "left")
    )

    # Pair level-start with next level-end (within 300 ft altitude tolerance)
    w_level = Window.partitionBy("flight_id").orderBy("event_timestamp_utc", "event_id")
    paired_segments = (
        level_events
        .withColumn("next_type", lead("event_type").over(w_level))
        .withColumn("next_t", lead("event_timestamp_utc").over(w_level))
        .withColumn("next_alt", lead("altitude_ft").over(w_level))
        .withColumn("next_lat", lead("latitude").over(w_level))
        .withColumn("next_lon", lead("longitude").over(w_level))
        .withColumn("next_flown_nm", lead("distance_flown_nm").over(w_level))
        .filter(
            (col("event_type") == "level-start")
            & (col("next_type") == "level-end")
            & (spark_abs(col("next_alt") - col("altitude_ft")) <= 300.0)
            & (col("next_t") >= col("event_timestamp_utc"))
        )
        .select(
            "flight_id", "dest_icao", "dest_elevation_ft",
            col("event_timestamp_utc").alias("seg_start_utc"),
            col("next_t").alias("seg_end_utc"),
            col("altitude_ft").alias("seg_alt_ft"),
            col("latitude").alias("seg_start_lat"),
            col("longitude").alias("seg_start_lon"),
            col("next_lat").alias("seg_end_lat"),
            col("next_lon").alias("seg_end_lon"),
            col("distance_flown_nm").alias("seg_start_flown_nm"),
            col("next_flown_nm").alias("seg_end_flown_nm"),
        )
    )

    # Fusion of consecutive chunks: |delta_alt| <= 200 ft and gap <= 60 s
    w_merge = Window.partitionBy("flight_id").orderBy("seg_start_utc")
    with_grouping = (
        paired_segments
        .withColumn("prev_end_utc", lag("seg_end_utc").over(w_merge))
        .withColumn("prev_alt_ft", lag("seg_alt_ft").over(w_merge))
        .withColumn("gap_s", unix_timestamp("seg_start_utc") - unix_timestamp("prev_end_utc"))
        .withColumn("alt_diff_ft", spark_abs(col("seg_alt_ft") - col("prev_alt_ft")))
        .withColumn(
            "is_new_group",
            when(
                col("prev_end_utc").isNull()
                | (col("gap_s") < 0)
                | (col("gap_s") > cfg.level_merge_max_gap_s)
                | (col("alt_diff_ft") > cfg.level_merge_max_alt_diff_ft),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn("group_id", spark_sum("is_new_group").over(w_merge))
    )

    # Aggregate fused segments
    merged_levels = (
        with_grouping
        .groupBy("flight_id", "dest_icao", "dest_elevation_ft", "group_id")
        .agg(
            spark_min("seg_start_utc").alias("start_utc"),
            spark_max("seg_end_utc").alias("end_utc"),
            spark_round(percentile_approx("seg_alt_ft", 0.5), 1).alias("altitude_ft"),
            # first chunk / last chunk of the level-off: min / max of a struct ordered by time (deterministic,
            # unlike first() / last(), whose result depends on the row order after a shuffle)
            spark_min(struct("seg_start_utc", "seg_start_lat", "seg_start_lon", "seg_start_flown_nm")).alias("_first"),
            spark_max(struct("seg_end_utc", "seg_end_lat", "seg_end_lon", "seg_end_flown_nm")).alias("_last"),
            spark_sum(lit(1)).alias("segments_merged"),
        )
        .withColumn("start_lat", col("_first.seg_start_lat"))
        .withColumn("start_lon", col("_first.seg_start_lon"))
        .withColumn("end_lat", col("_last.seg_end_lat"))
        .withColumn("end_lon", col("_last.seg_end_lon"))
        .withColumn("flown_start_nm", col("_first.seg_start_flown_nm"))
        .withColumn("flown_end_nm", col("_last.seg_end_flown_nm"))
        .withColumn(
            "duration_min",
            spark_round((unix_timestamp("end_utc") - unix_timestamp("start_utc")) / 60.0, 2),
        )
        .withColumn(
            "displacement_km",
            spark_round(spark_haversine_km(col("start_lat"), col("start_lon"), col("end_lat"), col("end_lon")), 2),
        )
        .withColumn(
            "distance_flown_km",
            when(
                col("flown_start_nm").isNotNull()
                & col("flown_end_nm").isNotNull()
                & (col("flown_end_nm") >= col("flown_start_nm")),
                spark_round((col("flown_end_nm") - col("flown_start_nm")) * 1.852, 2),
            ).otherwise(col("displacement_km")),
        )
        # Filter kept level segments: duration >= 30 s and altitude >= elevation + 1000 ft
        .filter(
            (col("duration_min") * 60.0 >= cfg.level_min_duration_s)
            & (col("altitude_ft") >= (col("dest_elevation_ft") + cfg.min_ground_clearance_ft))
        )
        .withColumn(
            "altitude_band",
            when(col("altitude_ft") < 7000.0, "<FL70")
            .when(col("altitude_ft") < 10000.0, "FL70-100")
            .when(col("altitude_ft") < 20000.0, "FL100-200")
            .otherwise(">=FL200"),
        )
        .withColumn(
            "is_holding",
            (col("duration_min") >= cfg.holding_min_duration_min)
            & (col("distance_flown_km") >= (lit(cfg.holding_flown_displacement_ratio) * col("displacement_km"))),
        )
        .withColumn("_silver_processed_at_utc", current_timestamp())
        .select(
            "flight_id", "dest_icao", "start_utc", "end_utc", "duration_min", "altitude_ft",
            "altitude_band",
            spark_round("start_lat", 4).alias("start_lat"),
            spark_round("start_lon", 4).alias("start_lon"),
            spark_round("end_lat", 4).alias("end_lat"),
            spark_round("end_lon", 4).alias("end_lon"),
            "distance_flown_km", "displacement_km", "is_holding",
            col("segments_merged").cast(LongType()).alias("segments_merged"),
            "_silver_processed_at_utc",
        )
    )

    parts = dict(dest_flights=dest_flights, flight_ring_entries=flight_ring_entries, flight_landings=flight_landings,
                 descent_bounds=descent_bounds, cached=[events_with_dest])
    return merged_levels, parts


def assemble_arrivals(settings, parts, level_offs):
    """Second half: silver/arrivals, one row per arrival, from the parts and the *written* level-offs table."""
    cfg = settings.arrivals
    dest_flights, flight_ring_entries = parts["dest_flights"], parts["flight_ring_entries"]
    flight_landings, descent_bounds = parts["flight_landings"], parts["descent_bounds"]

    # 7. Aggregate level-off metrics per flight for silver/arrivals
    flight_level_metrics = (
        level_offs
        .groupBy("flight_id")
        .agg(
            spark_sum(lit(1)).cast(LongType()).alias("level_off_count"),
            spark_round(spark_sum("duration_min"), 2).alias("level_minutes"),
            spark_round(spark_sum(when(col("altitude_band") == "<FL70", col("duration_min")).otherwise(lit(0.0))), 2).alias("level_minutes_lt_fl70"),
            spark_round(spark_sum(when(col("altitude_band") == "FL70-100", col("duration_min")).otherwise(lit(0.0))), 2).alias("level_minutes_fl70_100"),
            spark_round(spark_sum(when(col("altitude_band") == "FL100-200", col("duration_min")).otherwise(lit(0.0))), 2).alias("level_minutes_fl100_200"),
            spark_round(spark_sum(when(col("altitude_band") == ">=FL200", col("duration_min")).otherwise(lit(0.0))), 2).alias("level_minutes_ge_fl200"),
            spark_sum(when(col("is_holding"), lit(1)).otherwise(lit(0))).cast(LongType()).alias("holding_count"),
            spark_round(spark_sum(when(col("is_holding"), col("duration_min")).otherwise(lit(0.0))), 2).alias("holding_minutes"),
        )
    )

    # 8. Assemble silver/arrivals
    arrivals = (
        dest_flights
        .join(flight_ring_entries, "flight_id", "left")
        .join(flight_landings, "flight_id", "left")
        .join(descent_bounds.select("flight_id", "descent_start_utc"), "flight_id", "left")
        .join(flight_level_metrics, "flight_id", "left")
        .withColumn(
            "transit_min",
            when(
                col("landing_utc").isNotNull() & col("ring_entry_utc").isNotNull(),
                spark_round((unix_timestamp("landing_utc") - unix_timestamp("ring_entry_utc")) / 60.0, 2),
            ).otherwise(lit(None).cast(DoubleType())),
        )
        .withColumn(
            "is_transit_plausible",
            col("transit_min").between(cfg.min_transit_min, cfg.max_transit_min),
        )
        .withColumn("is_transit_plausible", coalesce(col("is_transit_plausible"), lit(False)))
        .withColumn("level_off_count", coalesce(col("level_off_count"), lit(0).cast(LongType())))
        .withColumn("level_minutes", coalesce(col("level_minutes"), lit(0.0)))
        .withColumn("level_minutes_lt_fl70", coalesce(col("level_minutes_lt_fl70"), lit(0.0)))
        .withColumn("level_minutes_fl70_100", coalesce(col("level_minutes_fl70_100"), lit(0.0)))
        .withColumn("level_minutes_fl100_200", coalesce(col("level_minutes_fl100_200"), lit(0.0)))
        .withColumn("level_minutes_ge_fl200", coalesce(col("level_minutes_ge_fl200"), lit(0.0)))
        .withColumn("holding_count", coalesce(col("holding_count"), lit(0).cast(LongType())))
        .withColumn("holding_minutes", coalesce(col("holding_minutes"), lit(0.0)))
        .withColumn(
            "arrival_date",
            to_date(coalesce(col("landing_utc"), col("descent_start_utc"), col("last_seen_utc"))),
        )
        .withColumn(
            "arrival_hour",
            hour(coalesce(col("landing_utc"), col("descent_start_utc"), col("last_seen_utc"))),
        )
        .withColumn("_silver_processed_at_utc", current_timestamp())
        .select(
            "flight_id", "dest_icao", "origin_icao", "operator_icao", "aircraft_typecode", "aircraft_class",
            "ring_entry_utc", "ring_entry_lat", "ring_entry_lon", "entry_sector",
            "landing_utc", "arrival_runway", "transit_min", "is_transit_plausible",
            "descent_start_utc", "level_off_count", "level_minutes",
            "level_minutes_lt_fl70", "level_minutes_fl70_100", "level_minutes_fl100_200", "level_minutes_ge_fl200",
            "holding_count", "holding_minutes", "arrival_date", "arrival_hour",
            "_silver_processed_at_utc", "_source_file", "_ingestion_run_id", "_source_sha256", "_source_period",
        )
    )

    return arrivals


def run(spark, settings):
    """Run arrivals module, write silver/descent_level_offs then silver/arrivals, return summary stats.

    Always reads the Silver tables written by the Silver layer (see compute_level_offs).
    """
    steps = Steps()
    s = settings.silver_dir
    silver_flights = spark.read.parquet(str(s / "flights"))
    silver_events = spark.read.parquet(str(s / "flight_events"))
    silver_airports = spark.read.parquet(str(s / "airports"))
    bronze_measurements = spark.read.parquet(str(settings.bronze_dir / "measurements"))

    # Windows over tens of millions of events: more, smaller shuffle partitions keep the memory of each task low
    # (AQE merges the small ones afterwards). Restored at the end.
    previous_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.shuffle.partitions", str(max(int(previous_partitions), 64)))
    try:
        with steps.step("compute arrivals & descent level-offs"):
            level_offs_df, parts = compute_level_offs(
                spark, settings, silver_flights, silver_events, silver_airports, bronze_measurements)

        with steps.step("write silver/descent_level_offs"):
            write_table(level_offs_df, s / "descent_level_offs")

        with steps.step("write silver/arrivals"):
            level_offs_written = spark.read.parquet(str(s / "descent_level_offs"))
            write_table(assemble_arrivals(settings, parts, level_offs_written), s / "arrivals", ["_source_period"])
    finally:
        for df in parts.get("cached", []) if "parts" in locals() else []:
            df.unpersist()
        spark.conf.set("spark.sql.shuffle.partitions", previous_partitions)

    # Read back to compute report metrics
    arr = spark.read.parquet(str(s / "arrivals"))
    lev = spark.read.parquet(str(s / "descent_level_offs"))

    n_arr = arr.count()
    n_ring = arr.filter(col("ring_entry_utc").isNotNull()).count()
    n_landing = arr.filter(col("landing_utc").isNotNull()).count()
    n_plausible = arr.filter(col("is_transit_plausible")).count()
    n_holdings = lev.filter(col("is_holding")).count()
    n_fused_levels = lev.count()

    pct_ring = round(n_ring / max(n_arr, 1) * 100.0, 1)
    pct_landing = round(n_landing / max(n_arr, 1) * 100.0, 1)
    pct_plausible = round(n_plausible / max(n_arr, 1) * 100.0, 1)

    # Focus airports breakdown
    focus_stats = {}
    for apt in FOCUS_AIRPORTS:
        apt_df = arr.filter(col("dest_icao") == apt)
        apt_n = apt_df.count()
        if apt_n > 0:
            apt_landing = apt_df.filter(col("landing_utc").isNotNull()).count()
            apt_ring = apt_df.filter(col("ring_entry_utc").isNotNull()).count()
            apt_plausible = apt_df.filter(col("is_transit_plausible")).count()
            focus_stats[apt] = {
                "arrivals": apt_n,
                "pct_ring_entry": round(apt_ring / apt_n * 100.0, 1),
                "pct_landing": round(apt_landing / apt_n * 100.0, 1),
                "pct_transit_plausible": round(apt_plausible / apt_n * 100.0, 1),
            }

    stats = {
        "arrivals_count": n_arr,
        "pct_ring_entry": pct_ring,
        "pct_landing": pct_landing,
        "pct_transit_plausible": pct_plausible,
        "fused_level_offs": n_fused_levels,
        "holdings_count": n_holdings,
        "focus_airports": focus_stats,
        "durations_s": steps.durations,
    }
    log.info("Arrivals: %s total, %.1f%% with ring entry, %.1f%% with landing, %.1f%% plausible transit",
             f"{n_arr:,}", pct_ring, pct_landing, pct_plausible)
    log.info("Level-offs: %s fused segments, %s holding patterns detected", f"{n_fused_levels:,}", f"{n_holdings:,}")
    return stats
