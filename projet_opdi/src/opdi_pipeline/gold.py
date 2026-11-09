"""Gold: airport operations for a dashboard (daily / hourly traffic, runway usage, taxi times), built with
Spark SQL (one query per table in opdi_pipeline/sql/), validated against Silver and published only if every check
passes.

Airport events carry their airport in the `info` JSON (`osm_airport`, ICAO code) and the airport element in
`osm_ref` (e.g. runway "07-25"), so each runway / parking event is attached to the departure or to the arrival
airport of its flight.
"""
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql.functions import avg, coalesce, col, countDistinct, desc, lit, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

from . import schemas
from .common import QualityGateError, Steps, free_gb, log, parquet_gb, utc_now, write_report, write_table

SQL_DIR = Path(__file__).resolve().parent / "sql"
GOLD_TABLES = (
    "airport_hourly_ops", "airport_daily_ops", "runway_usage", "airport_ground_times",
    "arrival_reference", "airport_arrival_hourly", "airport_arrival_daily", "holding_hotspots",
)
OUTPUT_FILES = {
    "airport_hourly_ops": 4, "airport_daily_ops": 1, "runway_usage": 2, "airport_ground_times": 1,
    "arrival_reference": 1, "airport_arrival_hourly": 2, "airport_arrival_daily": 1, "holding_hotspots": 1,
}
SHOWCASE_AIRPORTS = ["LFBO", "ENZV"]
FOCUS_AIRPORTS = ["LFPG", "LFBO", "ENZV", "EGLL", "EDDF"]


def sql_text(name):
    """Spark SQL query of a Gold step (opdi_pipeline/sql/<name>.sql)."""
    return (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


def register_silver_views(spark, settings):
    """Silver tables as temporary views, the inputs of the Gold queries."""
    s = settings.silver_dir
    spark.read.parquet(str(s / "flights")).createOrReplaceTempView("silver_flights")
    spark.read.parquet(str(s / "flight_events")).createOrReplaceTempView("silver_events")
    spark.read.parquet(str(s / "runways")).createOrReplaceTempView("silver_runways")
    spark.read.parquet(str(s / "arrivals")).createOrReplaceTempView("silver_arrivals")
    spark.read.parquet(str(s / "descent_level_offs")).createOrReplaceTempView("silver_descent_level_offs")
    (spark.read.parquet(str(s / "airports"))
        .select("airport_icao", "airport_name", "iso_country", "municipality")
        .createOrReplaceTempView("airports_dim"))


def build_movements(spark, settings):
    """Airport movements (one row per departure / arrival), cached and registered as the view airport_movements.

    flights_timed is used twice (departures, arrivals): it is cached too, so that the events are scanned once.
    """
    flights_timed = spark.sql(sql_text("flights_timed")).persist()
    flights_timed.createOrReplaceTempView("flights_timed")
    movements = spark.sql(sql_text("airport_movements"),
                          args={"scope_start": str(settings.start), "scope_end": str(settings.end)}).persist()
    movements.createOrReplaceTempView("airport_movements")
    return flights_timed, movements


def build_hourly_completeness(spark, movements):
    """Temporal regularity check: flag hours with < 50% of the European median traffic."""
    dates = movements.select("movement_date").distinct()
    hours = spark.range(0, 24).withColumnRenamed("id", "movement_hour")
    grid = dates.crossJoin(hours)
    euro_traffic = (
        grid
        .join(movements.groupBy("movement_date", "movement_hour").count(),
              ["movement_date", "movement_hour"], "left")
        .select(
            "movement_date", "movement_hour",
            coalesce(col("count"), lit(0)).cast("bigint").alias("total_euro_traffic"),
        )
    )
    euro_traffic.createOrReplaceTempView("euro_traffic_raw")
    medians = spark.sql("""
        SELECT movement_hour,
               percentile_approx(total_euro_traffic, 0.50D, 100) AS median_euro_traffic
        FROM euro_traffic_raw
        GROUP BY movement_hour
    """)
    medians.createOrReplaceTempView("euro_medians")
    completeness = spark.sql("""
        SELECT t.movement_date,
               t.movement_hour,
               t.total_euro_traffic,
               m.median_euro_traffic,
               (t.total_euro_traffic >= 0.50D * m.median_euro_traffic) AS is_complete_hour
        FROM euro_traffic_raw t
        JOIN euro_medians m ON t.movement_hour = m.movement_hour
    """).persist()
    completeness.createOrReplaceTempView("hourly_completeness")
    return completeness


# ---------------------------------------------------------------------------------------------- validation
def validate(gold, movements, silver_flights, spark, settings, tolerance=0.01):
    """Checks of the written Gold tables. Returns ({check: passed}, figures)."""
    daily, hourly, runway, ground = (gold[t] for t in ("airport_daily_ops", "airport_hourly_ops",
                                                        "runway_usage", "airport_ground_times"))
    arr_daily = gold["airport_arrival_daily"]
    arr_hourly = gold["airport_arrival_hourly"]
    arr_ref = gold["arrival_reference"]
    hotspots = gold["holding_hotspots"]

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

    # 4) Range and consistency checks for base tables
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

    # 5) Range and consistency checks for arrival efficiency tables
    silver_arr = spark.table("silver_arrivals")
    complete_hours = spark.table("hourly_completeness").filter(col("is_complete_hour"))
    expected_complete_arr = (
        silver_arr
        .join(complete_hours, (silver_arr.arrival_date == complete_hours.movement_date)
                             & (silver_arr.arrival_hour == complete_hours.movement_hour), "inner")
        .count()
    )
    daily_arr_tot = arr_daily.agg(spark_sum("arrivals")).first()[0] or 0
    checks["airport_arrival_daily: arrivals match silver_arrivals in complete hours"] = daily_arr_tot == expected_complete_arr

    invalid_arr_daily = arr_daily.filter(
        (col("arrivals") < 0) | (col("arrivals_measured") < 0) |
        (col("arrivals_measured") > col("arrivals")) |
        ~col("pct_coverage").between(0, 100) |
        (col("avg_level_offs") < 0) | (col("avg_level_minutes") < 0) |
        (col("holding_minutes") < 0) | (col("est_fuel_kg") < 0) | (col("est_co2_kg") < 0) |
        ~col("pct_with_holding").between(0, 100)
    ).count()
    checks["airport_arrival_daily: counts, coverages and positive metrics valid"] = invalid_arr_daily == 0

    invalid_arr_hourly = arr_hourly.filter(
        (col("arrivals") < 0) | (col("arrivals_measured") < 0) |
        (col("arrivals_measured") > col("arrivals")) |
        ~col("movement_hour").between(0, 23) |
        (col("avg_level_minutes") < 0) | (col("holdings") < 0)
    ).count()
    checks["airport_arrival_hourly: counts and hours valid"] = invalid_arr_hourly == 0

    invalid_ref = arr_ref.filter((col("ref_transit_min") <= 0) | (col("n_flights") < 1)).count()
    checks["arrival_reference: positive references and valid group size"] = invalid_ref == 0

    invalid_hotspots = hotspots.filter(
        (col("holdings") <= 0) | (col("holding_minutes") < 0) |
        ~col("lat_cell").between(-90, 90) | ~col("lon_cell").between(-180, 180)
    ).count()
    checks["holding_hotspots: valid counts and locations"] = invalid_hotspots == 0

    # 6) Coverage of the indicators (informative, for the report)
    cov = movements.agg(
        spark_round(avg(col("time_from_event").cast("int")) * 100, 1).alias("pct_movements_timed_by_event"),
        spark_round(avg(col("runway_ref").isNotNull().cast("int")) * 100, 1).alias("pct_movements_with_runway"),
        spark_round(avg(col("taxi_min").isNotNull().cast("int")) * 100, 1).alias("pct_movements_with_taxi_time"),
    ).first().asDict()
    rw = runway.agg(spark_sum("runway_movements").alias("all"),
                    spark_sum(when(col("length_ft").isNotNull(), col("runway_movements"))).alias("matched")).first()
    cov["pct_runway_movements_matched_ourairports"] = round((rw["matched"] or 0) / max(rw["all"] or 0, 1) * 100, 1)

    # 7) Showcase airports & Focus airports for arrival study
    showcase = {}
    for apt in SHOWCASE_AIRPORTS:
        s = daily.filter(col("airport_icao") == apt).agg(
            countDistinct("movement_date").alias("days"), spark_sum("movements").alias("movements"),
            spark_round(avg("movements"), 1).alias("per_day"), spark_round(avg("pct_jet"), 1).alias("pct_jet"),
            spark_round(avg("pct_time_from_event"), 1).alias("pct_time_from_event")).first()
        showcase[apt] = s.asDict()

    arrivals_focus = {}
    for apt in FOCUS_AIRPORTS:
        s = arr_daily.filter(col("airport_icao") == apt).agg(
            spark_sum("arrivals").alias("arrivals"),
            spark_sum("arrivals_measured").alias("arrivals_measured"),
            spark_round(avg("pct_coverage"), 1).alias("avg_pct_coverage"),
            spark_round(avg("avg_additional_min"), 2).alias("avg_additional_min"),
            spark_round(avg("avg_level_minutes"), 2).alias("avg_level_minutes"),
            spark_sum("holding_minutes").alias("holding_minutes"),
            spark_sum("est_fuel_kg").alias("est_fuel_kg"),
            spark_sum("est_co2_kg").alias("est_co2_kg"),
        ).first()
        if s and s["arrivals"]:
            arrivals_focus[apt] = s.asDict()

    incomplete_rows = (
        spark.table("hourly_completeness")
        .filter(~col("is_complete_hour"))
        .select("movement_date", "movement_hour", "total_euro_traffic", "median_euro_traffic")
        .orderBy("movement_date", "movement_hour")
        .collect()
    )
    incomplete_hours = [r.asDict() for r in incomplete_rows]

    figures = dict(movements=n_movements, departures=tot_dep, arrivals=tot_arr,
                   silver_flights_with_origin=expected_dep, silver_flights_with_destination=expected_arr,
                   coverage=cov, showcase=showcase, arrivals_focus=arrivals_focus,
                   incomplete_hours=incomplete_hours)
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
    register_silver_views(spark, settings)
    silver_fl = spark.table("silver_flights")

    flights_timed, movements = build_movements(spark, settings)
    with steps.step("airport movements"):
        by_source = {f"{r[0]} / {r[1]}": r[2] for r in
                     movements.groupBy("movement_type", "time_source").count().collect()}
        log.info("Airport movements by source of the movement time: %s", by_source)
    flights_timed.unpersist()

    completeness = build_hourly_completeness(spark, movements)

    g.mkdir(parents=True, exist_ok=True)
    for name in ("airport_hourly_ops", "airport_daily_ops", "runway_usage", "airport_ground_times"):
        with steps.step(f"write gold/{name}"):
            write_table(spark.sql(sql_text(name)), g / name, files=OUTPUT_FILES[name])

    with steps.step("write gold/arrival_reference"):
        write_table(spark.sql(sql_text("arrival_reference")), g / "arrival_reference", files=OUTPUT_FILES["arrival_reference"])
    spark.read.parquet(str(g / "arrival_reference")).createOrReplaceTempView("gold_arrival_reference")

    with steps.step("write gold/airport_arrival_hourly"):
        write_table(spark.sql(sql_text("airport_arrival_hourly")), g / "airport_arrival_hourly", files=OUTPUT_FILES["airport_arrival_hourly"])

    with steps.step("write gold/airport_arrival_daily"):
        write_table(spark.sql(sql_text("airport_arrival_daily"),
                              args={"fuel_coef": float(settings.arrivals.fuel_kg_per_level_min),
                                    "co2_factor": float(settings.arrivals.co2_factor)}),
                    g / "airport_arrival_daily", files=OUTPUT_FILES["airport_arrival_daily"])

    with steps.step("write gold/holding_hotspots"):
        write_table(spark.sql(sql_text("holding_hotspots")), g / "holding_hotspots", files=OUTPUT_FILES["holding_hotspots"])

    gold = {name: spark.read.parquet(str(g / name)) for name in GOLD_TABLES}

    log.info("Top 10 airports by movements over the period:")
    (gold["airport_daily_ops"].groupBy("airport_icao", "airport_name", "iso_country")
        .agg(spark_sum("movements").alias("movements"), spark_round(avg("movements"), 0).alias("avg_per_day"))
        .orderBy(desc("movements")).show(10, truncate=False))

    with steps.step("validation"):
        checks, figures = validate(gold, movements, silver_fl, spark, settings)
        schema_problems = {name: schemas.compare(df.schema, schemas.GOLD[name]) for name, df in gold.items()}
        for name, problems in schema_problems.items():
            checks[f"{name}: schema as expected"] = not problems
    for name, ok in checks.items():
        log.info("  %s  %s", "PASSED" if ok else "FAILED", name)
    for name, problems in schema_problems.items():
        for p in problems:
            log.error("  schema of gold/%s: %s", name, p)
    passed = all(checks.values())
    log.info("GOLD VALIDATION: %s | coverage: %s", "PASSED" if passed else "FAILED", figures["coverage"])

    published = []
    if passed:
        with steps.step("publication"):
            published = publish(spark, settings, gold)
    movements.unpersist()
    completeness.unpersist()
    log.info("Gold size: %.2f MB | free space left: %.1f GB", parquet_gb(g) * 1e3, free_gb(settings.data_root))
    report = write_report(settings, "gold", dict(
        validation_passed=passed, checks=checks, schema_problems=schema_problems,
        movements_by_time_source=by_source, **figures, published=published, durations_s=steps.durations))
    if not passed:
        raise QualityGateError("Gold validation failed: the Gold tables are not published")
    return report
