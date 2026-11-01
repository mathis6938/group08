"""Gold: airport operations for a dashboard (daily / hourly traffic, runway usage, taxi times), validated against
Silver and published only if every check passes.

Airport events carry their airport in the `info` JSON (`osm_airport`, ICAO code) and the airport element in
`osm_ref` (e.g. runway "07-25"), so each runway / parking event is attached to the departure or to the arrival
airport of its flight.
"""
from pyspark.sql import Row
from pyspark.sql.functions import (
    avg, broadcast, coalesce, col, concat_ws, count, countDistinct, current_timestamp, dayofweek, desc, expr, hour,
    lit, regexp_replace, struct, to_date, trim, unix_timestamp, upper, when)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

from .common import QualityGateError, Steps, free_gb, log, parquet_gb, utc_now, write_report, write_table

GOLD_TABLES = ("airport_hourly_ops", "airport_daily_ops", "runway_usage", "airport_ground_times")
GROUND_TYPES = ["take-off", "landing", "entry-runway", "exit-runway", "exit-parking_position", "entry-parking_position"]
SHOWCASE_AIRPORTS = ["LFBO", "ENZV"]


# ---------------------------------------------------------------------------------------------- movements
def airport_milestones(silver_events, silver_flights):
    """Per flight: take-off / landing times, departure runway (line-up), arrival runway (vacated), parking times."""
    ground_events = (silver_events
        .filter(col("event_type").isin(GROUND_TYPES))
        .select("flight_id", "event_type", col("event_timestamp_utc").alias("t"),
                col("info")["osm_airport"].alias("event_airport"), col("info")["osm_ref"].alias("element_ref"))
        .join(silver_flights.select("flight_id", "origin_icao", "dest_icao"), "flight_id"))

    at_origin = col("event_airport") == col("origin_icao")
    at_dest = col("event_airport") == col("dest_icao")

    def is_type(name):
        return col("event_type") == name

    return (ground_events
        .groupBy("flight_id")
        .agg(spark_min(when(is_type("take-off"), col("t"))).alias("takeoff_utc"),
             spark_max(when(is_type("landing"), col("t"))).alias("landing_utc"),
             # (time, runway) of the last runway entry at the departure airport = line-up before take-off
             spark_max(when(is_type("entry-runway") & at_origin, struct("t", "element_ref"))).alias("_dep_rwy"),
             # (time, runway) of the first runway exit at the arrival airport = runway vacated after landing
             spark_min(when(is_type("exit-runway") & at_dest, struct("t", "element_ref"))).alias("_arr_rwy"),
             spark_max(when(is_type("exit-parking_position") & at_origin, col("t"))).alias("exit_parking_utc"),
             spark_min(when(is_type("entry-parking_position") & at_dest, col("t"))).alias("entry_parking_utc"))
        .withColumn("dep_runway_utc", col("_dep_rwy.t")).withColumn("dep_runway_ref", col("_dep_rwy.element_ref"))
        .withColumn("arr_runway_utc", col("_arr_rwy.t")).withColumn("arr_runway_ref", col("_arr_rwy.element_ref"))
        .drop("_dep_rwy", "_arr_rwy"))


def minutes_between(start, end):
    """Duration in minutes, NULL outside [1, 120] min (not a plausible taxi time)."""
    m = spark_round((unix_timestamp(end) - unix_timestamp(start)) / 60.0, 1)
    return when(m.between(1, 120), m)


def flights_with_milestones(silver_flights, milestones):
    return (silver_flights
        .select("flight_id", "origin_icao", "dest_icao", "operator_icao", "aircraft_typecode",
                "aircraft_class", "flight_category", "first_seen_utc", "last_seen_utc")
        .join(milestones, "flight_id", "left")
        .withColumn("taxi_out_min", minutes_between(col("exit_parking_utc"), col("dep_runway_utc")))
        .withColumn("taxi_in_min", minutes_between(col("arr_runway_utc"), col("entry_parking_utc"))))


def airport_movements(flights_timed, start, end):
    """One row per airport movement: a flight gives a departure at its origin and an arrival at its destination.

    Movement time: runway event, else take-off / landing event, else first_seen / last_seen.
    """
    def movements(kind, airport_col, runway_time, runway_ref, event_time, fallback_time, taxi_col):
        return (flights_timed
            .filter(col(airport_col).isNotNull())
            .select("flight_id",
                    col(airport_col).alias("airport_icao"),
                    lit(kind).alias("movement_type"),
                    coalesce(col(runway_time), col(event_time), col(fallback_time)).alias("movement_time_utc"),
                    when(col(runway_time).isNotNull(), "runway event")
                        .when(col(event_time).isNotNull(), "take-off / landing event")
                        .otherwise("first / last seen").alias("time_source"),
                    (col(runway_time).isNotNull() | col(event_time).isNotNull()).alias("time_from_event"),
                    col(runway_ref).alias("runway_ref"),
                    col(taxi_col).alias("taxi_min"),
                    "operator_icao", "aircraft_typecode", "aircraft_class", "flight_category"))

    scope_start, scope_end = lit(str(start)).cast("timestamp"), lit(str(end)).cast("timestamp")
    return (movements("DEP", "origin_icao", "dep_runway_utc", "dep_runway_ref", "takeoff_utc", "first_seen_utc",
                      "taxi_out_min")
        .unionByName(
            movements("ARR", "dest_icao", "arr_runway_utc", "arr_runway_ref", "landing_utc", "last_seen_utc",
                      "taxi_in_min"))
        .filter((col("movement_time_utc") >= scope_start) & (col("movement_time_utc") < scope_end))
        .withColumn("movement_date", to_date("movement_time_utc"))
        .withColumn("movement_hour", hour("movement_time_utc"))
        # ICAO aircraft class, e.g. "L2J" = landplane, 2 engines, Jet: the 3rd character is the engine type
        .withColumn("is_jet", when(col("aircraft_class").isNotNull(), col("aircraft_class").substr(3, 1) == "J")))


# ---------------------------------------------------------------------------------------------- Gold tables
def airports_dimension(silver_airports):
    return silver_airports.select("airport_icao", "airport_name", "iso_country", "municipality")


def _dep_arr_counts():
    return [spark_sum((col("movement_type") == "DEP").cast("int")).alias("departures"),
            spark_sum((col("movement_type") == "ARR").cast("int")).alias("arrivals"),
            count("*").alias("movements")]


def hourly_ops(movements, airports_dim):
    """airport x day x hour (UTC)."""
    return (movements
        .groupBy("airport_icao", "movement_date", "movement_hour")
        .agg(*_dep_arr_counts())
        .join(broadcast(airports_dim), "airport_icao", "left")
        .withColumn("_gold_built_at_utc", current_timestamp()))


def daily_ops(movements, hourly, airports_dim):
    """airport x day. Averages ignore NULL: unknown aircraft class / country are left out of the shares."""
    peak_hour = hourly.groupBy("airport_icao", "movement_date").agg(spark_max("movements").alias("peak_hour_movements"))
    return (movements
        .groupBy("airport_icao", "movement_date")
        .agg(*_dep_arr_counts(),
             countDistinct("operator_icao").alias("distinct_operators"),
             countDistinct("aircraft_typecode").alias("distinct_aircraft_types"),
             spark_round(avg(col("is_jet").cast("int")) * 100, 1).alias("pct_jet"),
             spark_round(avg(when(col("flight_category") != "UNKNOWN",
                                  (col("flight_category") == "INTERNATIONAL").cast("int"))) * 100, 1)
                 .alias("pct_international"),
             spark_round(avg(col("time_from_event").cast("int")) * 100, 1).alias("pct_time_from_event"))
        .join(peak_hour, ["airport_icao", "movement_date"], "left")
        .join(broadcast(airports_dim), "airport_icao", "left")
        .withColumn("day_of_week", dayofweek("movement_date"))      # 1 = Sunday ... 7 = Saturday
        .withColumn("_gold_built_at_utc", current_timestamp()))


def normalise_runway(c):
    """OSM runway references look like "07-25", "14L/32R" or "07": normalised to "LE-HE"."""
    return regexp_replace(upper(trim(c)), r"\s*[/;]\s*|\s+", "-")


def runway_usage(movements, silver_runways):
    """airport x runway x day, with the runway characteristics of OurAirports (matched on both runway ends)."""
    rw_dim = (silver_runways
        .dropDuplicates(["airport_icao", "le_ident", "he_ident"])
        .select(col("airport_icao").alias("rw_airport_icao"),
                concat_ws("-", "le_ident", "he_ident").alias("rw_key"),
                concat_ws("-", "he_ident", "le_ident").alias("rw_key_reversed"),
                "le_ident", "he_ident", "length_ft", "width_ft", "surface", "is_closed"))
    counts = (movements
        .filter(col("runway_ref").isNotNull())
        .withColumn("runway_ident", normalise_runway(col("runway_ref")))
        .groupBy("airport_icao", "runway_ident", "movement_date")
        .agg(spark_sum(when(col("movement_type") == "DEP", 1).otherwise(0)).alias("runway_departures"),
             spark_sum(when(col("movement_type") == "ARR", 1).otherwise(0)).alias("runway_arrivals"),
             count("*").alias("runway_movements")))
    return (counts
        .join(broadcast(rw_dim),
              (col("airport_icao") == col("rw_airport_icao")) &
              col("runway_ident").isin(col("rw_key"), col("rw_key_reversed"), col("le_ident"), col("he_ident")),
              "left")
        .drop("rw_airport_icao", "rw_key", "rw_key_reversed", "le_ident", "he_ident")
        .withColumn("_gold_built_at_utc", current_timestamp()))


def ground_times(movements, airports_dim):
    """airport x day: taxi-out / taxi-in (mean, median, p90) and coverage (share of movements with a taxi time)."""
    def taxi_daily(kind, prefix, total_name):
        return (movements
            .filter(col("movement_type") == kind)
            .groupBy("airport_icao", "movement_date")
            .agg(count("*").alias(total_name),
                 count("taxi_min").alias(f"{prefix}_measured_flights"),
                 spark_round(avg("taxi_min"), 1).alias(f"avg_{prefix}_min"),
                 spark_round(expr("percentile_approx(taxi_min, 0.5)"), 1).alias(f"median_{prefix}_min"),
                 spark_round(expr("percentile_approx(taxi_min, 0.9)"), 1).alias(f"p90_{prefix}_min")))

    return (taxi_daily("DEP", "taxi_out", "total_departures")
        .join(taxi_daily("ARR", "taxi_in", "total_arrivals"), ["airport_icao", "movement_date"], "outer")
        .join(broadcast(airports_dim), "airport_icao", "left")
        .withColumn("pct_taxi_out_coverage",
            when(col("total_departures") > 0,
                 spark_round(col("taxi_out_measured_flights") / col("total_departures") * 100, 1)).otherwise(lit(0.0)))
        .withColumn("pct_taxi_in_coverage",
            when(col("total_arrivals") > 0,
                 spark_round(col("taxi_in_measured_flights") / col("total_arrivals") * 100, 1)).otherwise(lit(0.0)))
        .withColumn("_gold_built_at_utc", current_timestamp()))


# ---------------------------------------------------------------------------------------------- validation
def validate(gold, movements, silver_flights, tolerance=0.01):
    """Checks of the written Gold tables. Returns ({check: passed}, figures)."""
    daily, hourly, runway, ground = (gold[t] for t in ("airport_daily_ops", "airport_hourly_ops",
                                                        "runway_usage", "airport_ground_times"))
    checks = {}
    # 1) Completeness: no empty Gold table
    for name, df in gold.items():
        checks[f"{name} is not empty"] = df.limit(1).count() == 1

    # 2) The aggregations neither lose nor duplicate movements
    n_movements = movements.count()
    tot = daily.agg(spark_sum("departures").alias("dep"), spark_sum("arrivals").alias("arr"),
                    spark_sum("movements").alias("mov")).first()
    tot_dep, tot_arr, tot_mov = tot["dep"] or 0, tot["arr"] or 0, tot["mov"] or 0
    tot_hourly = hourly.agg(spark_sum("movements")).first()[0] or 0
    checks["daily movements = airport_movements"] = tot_mov == n_movements
    checks["hourly movements = airport_movements"] = tot_hourly == n_movements

    # 3) Reconciliation with Silver: each Silver flight with a known departure (arrival) airport gives one departure
    #    (arrival). Only flights whose movement time falls just outside the scope may be missing.
    expected_dep = silver_flights.filter(col("origin_icao").isNotNull()).count()
    expected_arr = silver_flights.filter(col("dest_icao").isNotNull()).count()

    def reconciles(gold_n, silver_n):
        return gold_n <= silver_n and (silver_n - gold_n) <= tolerance * max(silver_n, 1)

    checks["departures reconcile with Silver (gap <= 1 %)"] = reconciles(tot_dep, expected_dep)
    checks["arrivals reconcile with Silver (gap <= 1 %)"] = reconciles(tot_arr, expected_arr)
    log.info("Departures: Gold %s / Silver flights with a known origin %s", f"{tot_dep:,}", f"{expected_dep:,}")
    log.info("Arrivals  : Gold %s / Silver flights with a known destination %s", f"{tot_arr:,}", f"{expected_arr:,}")

    # 4) Range and consistency checks
    invalid_daily = daily.filter(
        (col("departures") < 0) | (col("arrivals") < 0) |
        (col("movements") != (col("departures") + col("arrivals"))) |
        (col("peak_hour_movements") > col("movements")) |
        ~col("pct_jet").between(0, 100) | ~col("pct_international").between(0, 100) |
        ~col("pct_time_from_event").between(0, 100)).count()
    invalid_hourly = hourly.filter(
        (col("departures") < 0) | (col("arrivals") < 0) |
        (col("movements") != (col("departures") + col("arrivals"))) | ~col("movement_hour").between(0, 23)).count()
    invalid_runway = runway.filter(
        (col("runway_movements") != col("runway_departures") + col("runway_arrivals")) |
        (col("runway_movements") <= 0)).count()
    runway_above_traffic = (runway
        .groupBy("airport_icao", "movement_date").agg(spark_sum("runway_movements").alias("rw_movements"))
        .join(daily.select("airport_icao", "movement_date", "movements"), ["airport_icao", "movement_date"], "left")
        .filter(col("movements").isNull() | (col("rw_movements") > col("movements")))
        .count())
    invalid_ground = ground.filter(
        ~col("pct_taxi_out_coverage").between(0, 100) | ~col("pct_taxi_in_coverage").between(0, 100) |
        (col("avg_taxi_out_min") < 0) | (col("avg_taxi_in_min") < 0) |
        (col("median_taxi_out_min") > col("p90_taxi_out_min")) | (col("median_taxi_in_min") > col("p90_taxi_in_min"))
    ).count()
    checks["airport_daily_ops: counts and percentages valid"] = invalid_daily == 0
    checks["airport_hourly_ops: counts and hours valid"] = invalid_hourly == 0
    checks["runway_usage: counts valid"] = invalid_runway == 0
    checks["runway_usage: never more than the airport traffic"] = runway_above_traffic == 0
    checks["airport_ground_times: durations and coverage valid"] = invalid_ground == 0

    # 5) Coverage of the indicators (informative, for the report)
    cov = movements.agg(
        spark_round(avg(col("time_from_event").cast("int")) * 100, 1).alias("pct_movements_timed_by_event"),
        spark_round(avg(col("runway_ref").isNotNull().cast("int")) * 100, 1).alias("pct_movements_with_runway"),
        spark_round(avg(col("taxi_min").isNotNull().cast("int")) * 100, 1).alias("pct_movements_with_taxi_time"),
    ).first().asDict()
    rw = runway.agg(spark_sum("runway_movements").alias("all"),
                    spark_sum(when(col("length_ft").isNotNull(), col("runway_movements"))).alias("matched")).first()
    cov["pct_runway_movements_matched_ourairports"] = round((rw["matched"] or 0) / max(rw["all"] or 0, 1) * 100, 1)

    # 6) Showcase airports (informative)
    showcase = {}
    for apt in SHOWCASE_AIRPORTS:
        s = daily.filter(col("airport_icao") == apt).agg(
            countDistinct("movement_date").alias("days"), spark_sum("movements").alias("movements"),
            spark_round(avg("movements"), 1).alias("per_day"), spark_round(avg("pct_jet"), 1).alias("pct_jet"),
            spark_round(avg("pct_time_from_event"), 1).alias("pct_time_from_event")).first()
        showcase[apt] = s.asDict()

    figures = dict(movements=n_movements, departures=tot_dep, arrivals=tot_arr,
                   silver_flights_with_origin=expected_dep, silver_flights_with_destination=expected_arr,
                   coverage=cov, showcase=showcase)
    return checks, figures


def publish(spark, settings, gold):
    """_SUCCESS marker + one record per table in gold/run_status (history of the publications)."""
    marker = settings.gold_dir / "_SUCCESS"
    marker.write_text(f"Gold published on {utc_now().isoformat()} (run {settings.run_id})\n")
    status = [Row(run_id=settings.run_id, table_name=name, scope_start=str(settings.start),
                  scope_end=str(settings.end), row_count=int(df.count()),
                  size_mb=round(parquet_gb(settings.gold_dir / name) * 1e3, 2), validation_status="PASSED",
                  published_at_utc=utc_now().isoformat()) for name, df in gold.items()]
    spark.createDataFrame(status).write.mode("append").parquet(str(settings.gold_dir / "run_status"))
    return [r.asDict() for r in status]


# ---------------------------------------------------------------------------------------------- layer
def run(spark, settings):
    steps = Steps()
    g = settings.gold_dir
    (g / "_SUCCESS").unlink(missing_ok=True)          # Gold is "not ready" until it is published again
    silver_fl = spark.read.parquet(str(settings.silver_dir / "flights"))
    silver_ev = spark.read.parquet(str(settings.silver_dir / "flight_events"))
    airports_dim = airports_dimension(spark.read.parquet(str(settings.silver_dir / "airports")))
    silver_rw = spark.read.parquet(str(settings.silver_dir / "runways"))

    # Movements (cached: reused by every Gold table). flights_timed is used twice (DEP, ARR): cached too,
    # so that the events are scanned only once.
    flights_timed = flights_with_milestones(silver_fl, airport_milestones(silver_ev, silver_fl)).persist()
    movements = airport_movements(flights_timed, settings.start, settings.end).persist()
    with steps.step("airport movements"):
        by_source = {f"{r[0]} / {r[1]}": r[2] for r in
                     movements.groupBy("movement_type", "time_source").count().collect()}
        log.info("Airport movements by source of the movement time: %s", by_source)
    flights_timed.unpersist()

    hourly = hourly_ops(movements, airports_dim)
    tables = {"airport_hourly_ops": (hourly, 4),
              "airport_daily_ops": (daily_ops(movements, hourly, airports_dim), 1),
              "runway_usage": (runway_usage(movements, silver_rw), 2),
              "airport_ground_times": (ground_times(movements, airports_dim), 1)}
    g.mkdir(parents=True, exist_ok=True)
    for name, (df, n_files) in tables.items():          # small tables: few output files
        with steps.step(f"write gold/{name}"):
            write_table(df, g / name, files=n_files)
    gold = {name: spark.read.parquet(str(g / name)) for name in GOLD_TABLES}

    log.info("Top 10 airports by movements over the period:")
    (gold["airport_daily_ops"].groupBy("airport_icao", "airport_name", "iso_country")
        .agg(spark_sum("movements").alias("movements"), spark_round(avg("movements"), 0).alias("avg_per_day"))
        .orderBy(desc("movements")).show(10, truncate=False))

    with steps.step("validation"):
        checks, figures = validate(gold, movements, silver_fl)
    for name, ok in checks.items():
        log.info("  %s  %s", "PASSED" if ok else "FAILED", name)
    passed = all(checks.values())
    log.info("GOLD VALIDATION: %s | coverage: %s", "PASSED" if passed else "FAILED", figures["coverage"])

    published = []
    if passed:
        with steps.step("publication"):
            published = publish(spark, settings, gold)
    movements.unpersist()
    log.info("Gold size: %.2f MB | free space left: %.1f GB", parquet_gb(g) * 1e3, free_gb(settings.data_root))
    report = write_report(settings, "gold", dict(
        validation_passed=passed, checks=checks, movements_by_time_source=by_source, **figures,
        published=published, durations_s=steps.durations))
    if not passed:
        raise QualityGateError("Gold validation failed: the Gold tables are not published")
    return report
