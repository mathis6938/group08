"""Silver: standardisation, quality rules (quarantine or flag), deduplication, enrichment, and before/after evidence.

Grain: one row per flight (silver/flights), with its events (silver/flight_events) and two reference dimensions
(silver/airports, silver/runways). Rejected rows go to quarantine/ with the first rule they fail.
"""
from pyspark.sql import Window
from pyspark.sql.functions import (
    asin, avg, broadcast, coalesce, col, concat_ws, cos, current_timestamp, desc, expr, from_json, length, lit,
    lower, month, radians, row_number, sin, sqrt, to_date, to_timestamp, trim, unix_timestamp, upper, when, year)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, LongType, MapType, StringType

from . import schemas
from .common import QualityGateError, Steps, free_gb, log, parquet_gb, write_report, write_table


# ---------------------------------------------------------------------------------------------- cleaning helpers
def clean_str(c):
    """Trimmed string; '', 'nan' and NULL all become NULL (a single representation of a missing value)."""
    v = trim(col(c))
    return when((v == "") | (lower(v) == "nan"), None).otherwise(v)


def icao_code(c):
    """ICAO airport code: upper case, 4 characters, otherwise NULL."""
    v = upper(clean_str(c))
    return when(length(v) == 4, v)


def in_scope(ts, start, end):
    """Time in [start, end[ (UTC). Unparsable times (NULL) are kept, so that the quality rules quarantine them."""
    scope_start = lit(str(start)).cast("timestamp")
    scope_end = lit(str(end)).cast("timestamp")
    return ts.isNull() | ((ts >= scope_start) & (ts < scope_end))


# ---------------------------------------------------------------------------------------------- standardisation
def standardize_airports(bronze_airports):
    """OurAirports airports with a 4-character ident (the ICAO code when the airport has one)."""
    return (bronze_airports
        .filter(length(trim(col("ident"))) == 4)
        .select(
            upper(trim(col("ident"))).alias("airport_icao"),
            clean_str("name").alias("airport_name"),
            lower(clean_str("type")).alias("airport_type"),
            upper(clean_str("iso_country")).alias("iso_country"),
            upper(clean_str("continent")).alias("continent"),
            clean_str("municipality").alias("municipality"),
            col("latitude_deg").cast(DoubleType()).alias("latitude"),
            col("longitude_deg").cast(DoubleType()).alias("longitude"),
            col("elevation_ft").cast(DoubleType()).alias("elevation_ft"),
            upper(clean_str("iata_code")).alias("iata_code"))
        .dropDuplicates(["airport_icao"]))


def standardize_runways(bronze_runways, airports):
    """OurAirports runways (both ends: le_ident, he_ident) of the airports kept."""
    return (bronze_runways
        .select(
            upper(trim(col("airport_ident"))).alias("airport_icao"),
            upper(clean_str("le_ident")).alias("le_ident"),
            upper(clean_str("he_ident")).alias("he_ident"),
            col("length_ft").cast(DoubleType()).alias("length_ft"),
            col("width_ft").cast(DoubleType()).alias("width_ft"),
            upper(clean_str("surface")).alias("surface"),
            (trim(col("closed")) == "1").alias("is_closed"),
            col("le_heading_degT").cast(DoubleType()).alias("le_heading_deg"),
            col("he_heading_degT").cast(DoubleType()).alias("he_heading_deg"))
        .join(airports.select("airport_icao"), "airport_icao", "left_semi"))


def standardize_flights(bronze_flights, start, end):
    """Explicit target schema of the flights (one schema variant only, checked in Bronze) + scope column."""
    return (bronze_flights
        .select(
            col("id").cast(LongType()).alias("flight_id"),
            lower(clean_str("icao24")).alias("transponder_icao24"),
            upper(clean_str("flt_id")).alias("callsign"),
            upper(clean_str("registration")).alias("registration"),
            icao_code("adep").alias("origin_icao"),
            icao_code("ades").alias("dest_icao"),
            upper(clean_str("typecode")).alias("aircraft_typecode"),
            clean_str("model").alias("aircraft_model"),
            upper(clean_str("icao_aircraft_class")).alias("aircraft_class"),
            upper(clean_str("icao_operator")).alias("operator_icao"),
            to_timestamp("first_seen").alias("first_seen_utc"),      # session time zone = UTC
            to_timestamp("last_seen").alias("last_seen_utc"),
            "_source_file", "_source_period", "_ingestion_run_id", "_source_sha256")
        .withColumn("flight_date", to_date("first_seen_utc"))
        .withColumn("flight_year", year("first_seen_utc"))
        .withColumn("flight_month", month("first_seen_utc"))
        .withColumn("duration_min",
                    spark_round((unix_timestamp("last_seen_utc") - unix_timestamp("first_seen_utc")) / 60.0, 2))
        .withColumn("_in_scope", in_scope(col("first_seen_utc"), start, end)))


def standardize_events(bronze_events, start, end):
    """Explicit target schema of the events; the `info` JSON is parsed into a map, its "nan" values removed."""
    return (bronze_events
        .select(
            col("id").cast(LongType()).alias("event_id"),
            col("flight_id").cast(LongType()).alias("flight_id"),
            lower(clean_str("type")).alias("event_type"),
            to_timestamp("event_time").alias("event_timestamp_utc"),
            col("latitude").cast(DoubleType()).alias("latitude"),
            col("longitude").cast(DoubleType()).alias("longitude"),
            col("altitude").cast(DoubleType()).alias("altitude_ft"),
            clean_str("source").alias("source"),
            from_json(clean_str("info"), MapType(StringType(), StringType())).alias("info"),
            "_source_file", "_source_period", "_ingestion_run_id", "_source_sha256")
        .withColumn("info", expr("map_filter(info, (k, v) -> v IS NOT NULL AND lower(v) <> 'nan')"))
        .withColumn("event_date", to_date("event_timestamp_utc"))
        .withColumn("_in_scope", in_scope(col("event_timestamp_utc"), start, end)))


# ---------------------------------------------------------------------------------------------- quality rules
def flag_flights(flights):
    """Standardized flights in scope -> same rows + quarantine reason (NULL = valid) + flags."""
    return (flights
        # Reject rules: the first failing rule gives the quarantine reason
        .withColumn("_quarantine_reason",
            when(col("flight_id").isNull(), "MISSING_FLIGHT_ID")
            .when(col("transponder_icao24").isNull(), "MISSING_ICAO24")
            .when(col("first_seen_utc").isNull() | col("last_seen_utc").isNull(), "UNPARSABLE_TIMESTAMP")
            .when(col("first_seen_utc") > col("last_seen_utc"), "INVALID_CHRONOLOGY")
            .when(col("duration_min") > 1440, "DURATION_GT_24H"))
        # Flag-and-keep rules: the flight is valid, but an attribute is incomplete or suspicious
        .withColumn("has_adep", col("origin_icao").isNotNull())
        .withColumn("has_ades", col("dest_icao").isNotNull())
        .withColumn("is_short_flight", col("duration_min") < 2)
        .withColumn("_silver_flagged_at_utc", current_timestamp()))


def flag_events(events, valid_flight_ids):
    """Standardized events in scope + ids of the valid flights -> same rows + quarantine reason + flag."""
    known = valid_flight_ids.select("flight_id").distinct().withColumn("_flight_known", lit(True))
    return (events
        .join(known, "flight_id", "left")
        .withColumn("_quarantine_reason",
            when(col("flight_id").isNull(), "MISSING_FLIGHT_ID")
            .when(col("event_type").isNull(), "MISSING_EVENT_TYPE")
            .when(col("event_timestamp_utc").isNull(), "UNPARSABLE_TIMESTAMP")
            .when(~col("latitude").between(-90, 90) | ~col("longitude").between(-180, 180), "INVALID_COORDINATES")
            .when(col("_flight_known").isNull(), "ORPHAN_EVENT"))
        .drop("_flight_known")
        # Flag and keep: implausible altitude (below the lowest airports or above airliner ceilings)
        .withColumn("is_altitude_valid", col("altitude_ft").between(-1500, 60000))
        .withColumn("_silver_flagged_at_utc", current_timestamp()))


# ---------------------------------------------------------------------------------------------- deduplication
def dedup_flights(flights):
    """One row per flight_id: the most complete one (both airports known first), then the most recent file."""
    w = Window.partitionBy("flight_id").orderBy(
        (col("has_adep").cast("int") + col("has_ades").cast("int")).desc(), col("_source_period").desc())
    return flights.withColumn("_row_num", row_number().over(w)).filter(col("_row_num") == 1).drop("_row_num")


def dedup_events(events):
    """Same flight, same type, same time = same event.

    dropDuplicates is a hash aggregation (no sort), cheaper than a row_number() window on ~86 M rows. The events
    are already shuffled by flight_id for the join of flag_events, and Spark can reuse that partitioning.
    """
    return events.dropDuplicates(["flight_id", "event_type", "event_timestamp_utc"])


# ---------------------------------------------------------------------------------------------- enrichment
SILVER_FLIGHT_COLUMNS = [
    "flight_id", "callsign", "transponder_icao24", "registration",
    "aircraft_typecode", "aircraft_model", "aircraft_class", "operator_icao",
    "origin_icao", "dest_icao", "route", "flight_category",
    "origin_airport_name", "origin_country", "origin_municipality", "origin_lat", "origin_lon",
    "dest_airport_name", "dest_country", "dest_municipality", "dest_lat", "dest_lon",
    "distance_km", "duration_min", "first_seen_utc", "last_seen_utc",
    "flight_date", "flight_year", "flight_month",
    "has_adep", "has_ades", "is_short_flight",
    "_silver_processed_at_utc", "_source_file", "_source_period", "_ingestion_run_id", "_source_sha256"]

SILVER_EVENT_COLUMNS = [
    "event_id", "flight_id", "event_type", "event_timestamp_utc", "event_date",
    "latitude", "longitude", "altitude_ft", "is_altitude_valid", "source", "info",
    "_silver_processed_at_utc", "_source_file", "_source_period", "_ingestion_run_id", "_source_sha256"]


def airport_side(airports, prefix):
    """Airport dimension renamed for the departure side (origin_*) or the arrival side (dest_*)."""
    return airports.select(
        col("airport_icao").alias(f"{prefix}_icao_key"),
        col("airport_name").alias(f"{prefix}_airport_name"),
        col("iso_country").alias(f"{prefix}_country"),
        col("municipality").alias(f"{prefix}_municipality"),
        col("latitude").alias(f"{prefix}_lat"),
        col("longitude").alias(f"{prefix}_lon"))


def enrich_flights(flights, airports):
    """Departure and arrival airports (broadcast joins: the dimension is small), route, category, distance."""
    return (flights
        .join(broadcast(airport_side(airports, "origin")), col("origin_icao") == col("origin_icao_key"), "left")
        .join(broadcast(airport_side(airports, "dest")), col("dest_icao") == col("dest_icao_key"), "left")
        .drop("origin_icao_key", "dest_icao_key")
        .withColumn("route", concat_ws("-", coalesce(col("origin_icao"), lit("????")),
                                             coalesce(col("dest_icao"), lit("????"))))
        .withColumn("flight_category",
            when(col("origin_country").isNull() | col("dest_country").isNull(), "UNKNOWN")
            .when(col("origin_country") == col("dest_country"), "DOMESTIC")
            .otherwise("INTERNATIONAL"))
        # Great-circle distance between the two airports (haversine formula, km)
        .withColumn("_a",
            sin(radians(col("dest_lat") - col("origin_lat")) / 2) ** 2 +
            cos(radians(col("origin_lat"))) * cos(radians(col("dest_lat"))) *
            sin(radians(col("dest_lon") - col("origin_lon")) / 2) ** 2)
        .withColumn("distance_km", spark_round(lit(2 * 6371.0) * asin(sqrt(col("_a"))), 2))
        .drop("_a")
        .withColumn("_silver_processed_at_utc", current_timestamp())
        .select(*SILVER_FLIGHT_COLUMNS))


def finalize_events(events):
    return events.withColumn("_silver_processed_at_utc", current_timestamp()).select(*SILVER_EVENT_COLUMNS)


# ---------------------------------------------------------------------------------------------- evidence
def evidence(spark, settings, bronze_flights, bronze_events, std_flights, std_events, quar_schemas):
    """Before/after quality figures, computed from the written tables (they describe exactly what was published)."""
    silver_fl = spark.read.parquet(str(settings.silver_dir / "flights"))
    silver_ev = spark.read.parquet(str(settings.silver_dir / "flight_events"))
    # explicit schema: a quarantine table can be empty (no Parquet file to infer the schema from)
    quar_fl = spark.read.schema(quar_schemas["flights"]).parquet(str(settings.quarantine_dir / "flights"))
    quar_ev = spark.read.schema(quar_schemas["flight_events"]).parquet(str(settings.quarantine_dir / "flight_events"))

    # 1) Row accounting: Bronze = out of scope + quarantined + duplicates removed + Silver
    acc = {}
    for name, bronze_df, std_df, quar_df, silver_df in [
            ("flights", bronze_flights, std_flights, quar_fl, silver_fl),
            ("events", bronze_events, std_events, quar_ev, silver_ev)]:
        b_n, out_n = bronze_df.count(), std_df.filter(~col("_in_scope")).count()
        q_n, s_n = quar_df.count(), silver_df.count()
        acc[name] = dict(bronze=b_n, out_of_scope=out_n, quarantined=q_n, duplicates=b_n - out_n - q_n - s_n,
                         silver=s_n)
    log.info("--- Row accounting ---   %22s %16s", "flights", "flight_events")
    for key in ("bronze", "out_of_scope", "quarantined", "duplicates", "silver"):
        log.info("  %-20s %14s %16s", key, f"{acc['flights'][key]:,}", f"{acc['events'][key]:,}")
    consistent = all(v["duplicates"] >= 0 for v in acc.values())

    # 2) Quarantine reasons
    reasons = {name: {r[0]: r[1] for r in df.groupBy("_quarantine_reason").count().collect()}
               for name, df in [("flights", quar_fl), ("flight_events", quar_ev)]}
    log.info("Quarantine reasons: %s", reasons)

    # 3) Missing values: three forms in Bronze (NULL, '', 'nan') -> one form in Silver (NULL)
    pairs = [("flt_id", "callsign"), ("icao_operator", "operator_icao"), ("typecode", "aircraft_typecode"),
             ("adep", "origin_icao"), ("ades", "dest_icao")]
    b = bronze_flights.select(
        *[spark_sum(col(src).isNull().cast("int")).alias(f"{src}_null") for src, _ in pairs],
        *[spark_sum((trim(col(src)) == "").cast("int")).alias(f"{src}_empty") for src, _ in pairs],
        *[spark_sum((lower(trim(col(src))) == "nan").cast("int")).alias(f"{src}_nan") for src, _ in pairs]).first()
    s = silver_fl.select(*[spark_sum(col(dst).isNull().cast("int")).alias(dst) for _, dst in pairs]).first()
    n_bronze, n_silver = max(acc["flights"]["bronze"], 1), max(acc["flights"]["silver"], 1)
    missing = [dict(bronze_column=src, bronze_pct_null=round(b[src + "_null"] / n_bronze * 100, 2),
                    bronze_pct_empty=round(b[src + "_empty"] / n_bronze * 100, 2),
                    bronze_pct_nan=round(b[src + "_nan"] / n_bronze * 100, 2),
                    silver_column=dst, silver_pct_null=round(s[dst] / n_silver * 100, 2)) for src, dst in pairs]
    log.info("--- Missing values (%%): Bronze NULL / empty / 'nan' -> Silver NULL ---")
    for m in missing:
        log.info("  %-14s %6.2f %6.2f %6.2f  -> %-18s %6.2f", m["bronze_column"], m["bronze_pct_null"],
                 m["bronze_pct_empty"], m["bronze_pct_nan"], m["silver_column"], m["silver_pct_null"])

    # 4) Completeness and flags of the Silver flights
    completeness = silver_fl.agg(
        spark_round(avg((col("has_adep") & col("has_ades")).cast("int")) * 100, 2).alias("pct_both_airports_known"),
        spark_round(avg(col("is_short_flight").cast("int")) * 100, 2).alias("pct_short_flights"),
        spark_round(avg(col("origin_lat").isNotNull().cast("int")) * 100, 2).alias("pct_origin_found_in_ourairports"),
    ).first().asDict()
    categories = {r[0]: r[1] for r in silver_fl.groupBy("flight_category").count().collect()}
    log.info("Completeness: %s | categories: %s", completeness, categories)

    # 5) Referential integrity: every Silver event belongs to a Silver flight
    orphans = (silver_ev.select("flight_id").distinct()
               .join(silver_fl.select("flight_id"), "flight_id", "left_anti").count())
    log.info("Silver events whose flight is not in Silver flights: %s", f"{orphans:,}")

    # 6) Events: types and altitude flag
    event_types = {r[0]: r[1] for r in silver_ev.groupBy("event_type").count().orderBy(desc("count")).collect()}
    pct_altitude_valid = silver_ev.agg(avg(col("is_altitude_valid").cast("int"))).first()[0]

    # 7) Types before vs after
    types = [dict(bronze=f"{src} {dict(df_b.dtypes)[src]}", silver=f"{dst} {dict(df_s.dtypes)[dst]}")
             for src, dst, df_b, df_s in [("first_seen", "first_seen_utc", bronze_flights, silver_fl),
                                          ("event_time", "event_timestamp_utc", bronze_events, silver_ev),
                                          ("info", "info", bronze_events, silver_ev)]]
    return dict(row_accounting=acc, accounting_consistent=consistent, quarantine_reasons=reasons,
                missing_values=missing, completeness=completeness, flight_categories=categories,
                orphan_events=orphans, event_types=event_types,
                pct_altitude_valid=round((pct_altitude_valid or 0) * 100, 2), types=types)


# ---------------------------------------------------------------------------------------------- layer
def run(spark, settings):
    steps = Steps()
    bronze = settings.bronze_dir
    bronze_flights = spark.read.parquet(str(bronze / "flight_list"))
    bronze_events = spark.read.parquet(str(bronze / "flight_events"))

    # Standardisation (lazy: nothing is computed yet)
    airports = standardize_airports(spark.read.parquet(str(bronze / "airports")))
    runways = standardize_runways(spark.read.parquet(str(bronze / "runways")), airports)
    std_flights = standardize_flights(bronze_flights, settings.start, settings.end)
    std_events = standardize_events(bronze_events, settings.start, settings.end)

    # Quality rules. The flagged flights (~3.3 M rows) are reused several times: cached once.
    flights_flagged = flag_flights(std_flights.filter(col("_in_scope")).drop("_in_scope")).persist()
    flights_valid = flights_flagged.filter(col("_quarantine_reason").isNull())
    flights_quarantine = flights_flagged.filter(col("_quarantine_reason").isNotNull())
    with steps.step("flight rules"):
        outcomes = {r[0]: r[1] for r in flights_flagged.groupBy(
            coalesce(col("_quarantine_reason"), lit("VALID"))).count().collect()}
        log.info("Flights in scope, by quality outcome: %s", outcomes)

    # Events: only kept if their flight is a valid Silver flight (referential integrity). The ~86 M events are
    # processed when they are written, and counted afterwards from the written tables (no count() before).
    events_flagged = flag_events(std_events.filter(col("_in_scope")).drop("_in_scope"),
                                 flights_valid.select("flight_id"))
    events_valid = events_flagged.filter(col("_quarantine_reason").isNull())
    events_quarantine = events_flagged.filter(col("_quarantine_reason").isNotNull())

    silver_flights = enrich_flights(dedup_flights(flights_valid), airports)
    silver_events = finalize_events(dedup_events(events_valid))

    # Write (static overwrite: Silver is rebuilt as a whole for the scope)
    s, q = settings.silver_dir, settings.quarantine_dir
    with steps.step("write silver/flights"):
        write_table(silver_flights, s / "flights", ["flight_year", "flight_month"])
    with steps.step("write silver/airports + runways"):
        write_table(airports, s / "airports")
        write_table(runways, s / "runways")
    with steps.step("write silver/flight_events"):
        write_table(silver_events, s / "flight_events", ["_source_period"])
    with steps.step("write quarantine/flights"):
        write_table(flights_quarantine, q / "flights", ["_source_period"])
    with steps.step("write quarantine/flight_events"):
        write_table(events_quarantine, q / "flight_events", ["_source_period"])
    quar_schemas = {"flights": flights_quarantine.schema, "flight_events": events_quarantine.schema}
    flights_flagged.unpersist()

    with steps.step("arrivals"):
        from . import arrivals
        arr_stats = arrivals.run(spark, settings, silver_flights, silver_events, airports)
        steps.durations.update({f"arrivals: {k}": v for k, v in arr_stats.pop("durations_s", {}).items()})

    log.info("Silver: %.2f GB | quarantine: %.3f GB | free space left: %.1f GB",
             parquet_gb(s), parquet_gb(q), free_gb(settings.data_root))

    with steps.step("before/after evidence"):
        ev = evidence(spark, settings, bronze_flights, bronze_events, std_flights, std_events, quar_schemas)
    schema_problems = {t: schemas.compare(spark.read.parquet(str(s / t)).schema, schemas.SILVER[t])
                       for t in schemas.SILVER}
    for t, problems in schema_problems.items():
        for p in problems:
            log.error("  schema of silver/%s: %s", t, p)
    report = write_report(settings, "silver", dict(
        flight_outcomes=outcomes, **ev, arrivals=arr_stats, schema_problems=schema_problems,
        size_gb=round(parquet_gb(s), 3), quarantine_size_gb=round(parquet_gb(q), 3),
        durations_s=steps.durations))
    if not ev["accounting_consistent"]:
        raise QualityGateError("Silver row accounting inconsistent (negative number of duplicates)")
    if ev["orphan_events"]:
        raise QualityGateError(f"{ev['orphan_events']} Silver events have no Silver flight")
    if any(schema_problems.values()):
        raise QualityGateError(f"Silver tables do not match their schema contract: {schema_problems}")
    return report
