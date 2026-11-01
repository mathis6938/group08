"""Performance measurements (used by notebooks/03_performance.ipynb): physical plans and timed comparisons.

Each comparison runs the same query in two ways and times it with the "noop" writer, which executes the whole
query without writing anything.
"""
import datetime
import time

from pyspark.sql.functions import broadcast, col

from . import silver
from .ingestion import windows


def timed(df):
    t0 = time.time()
    df.write.format("noop").mode("overwrite").save()
    return round(time.time() - t0, 2)


def plan_lines(df, keyword):
    plan = df._jdf.queryExecution().executedPlan().toString()
    return [line.strip() for line in plan.split("\n") if keyword in line]


def probe_window(settings):
    """Last 10-day window of the scope (used to keep the measurements short)."""
    start = max(w for w, _ in windows(settings.start, settings.end, settings.origin))
    return start, start + datetime.timedelta(days=10), int(start.strftime("%Y%m%d"))


def enrichment_joins(spark, settings):
    """Joins of the Silver flight enrichment in the physical plan (expected: BroadcastHashJoin, no shuffle)."""
    b = settings.bronze_dir
    airports = silver.standardize_airports(spark.read.parquet(str(b / "airports")))
    flights = silver.standardize_flights(spark.read.parquet(str(b / "flight_list")), settings.start, settings.end)
    flights = silver.dedup_flights(silver.flag_flights(flights.drop("_in_scope")))
    return plan_lines(silver.enrich_flights(flights, airports), "Join")     # planned only, not executed


def join_strategies(spark, settings):
    """Events of one window x valid flight ids: sort-merge join (both sides shuffled) vs broadcast join."""
    _, _, period = probe_window(settings)
    events = (spark.read.parquet(str(settings.bronze_dir / "flight_events"))
              .filter(col("_source_period") == period).select("flight_id", "type"))
    ids = spark.read.parquet(str(settings.silver_dir / "flights")).select("flight_id")
    events.count()                                                 # warm-up (file cache)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)    # forbid the automatic broadcast
    try:
        smj = timed(events.join(ids, "flight_id", "left_semi"))
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    bhj = timed(events.join(broadcast(ids), "flight_id", "left_semi"))
    return dict(window=period, events=events.count(), sort_merge_join_s=smj, broadcast_join_s=bhj,
                speed_up=round(smj / max(bhj, 0.01), 1))


def partition_pruning(spark, settings):
    """Same 10-day window of silver/flight_events: filter on the partition column vs on the event time."""
    start, end, period = probe_window(settings)
    ev = spark.read.parquet(str(settings.silver_dir / "flight_events"))
    pruned = ev.filter(col("_source_period") == period)
    scanned = ev.filter((col("event_timestamp_utc") >= str(start)) & (col("event_timestamp_utc") < str(end)))
    filters = [line[line.index("PartitionFilters"):][:160] for line in plan_lines(pruned, "PartitionFilters")]
    n_partitions = ev.select("_source_period").distinct().count()
    t_pruned, t_scanned = timed(pruned), timed(scanned)
    return dict(window=period, partitions=n_partitions, partition_filters=filters, pruned_s=t_pruned,
                all_partitions_s=t_scanned, speed_up=round(t_scanned / max(t_pruned, 0.01), 1))


def spark_configuration(spark):
    conf = {}
    for key in ["spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions", "spark.driver.memory",
                "spark.sql.autoBroadcastJoinThreshold"]:
        try:
            conf[key] = spark.conf.get(key)
        except Exception:
            conf[key] = "(Spark default)"
    return conf
