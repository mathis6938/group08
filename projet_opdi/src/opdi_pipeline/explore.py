"""Exploration of the arrival phase (not a pipeline layer): checks the assumptions of the arrival-efficiency study
on the real data before the Silver / Gold code is written.

    python projet_opdi/run_pipeline.py --env dev --layer explore

Reads silver/flights, silver/flight_events and bronze/measurements; writes reports/<run id>/explore.json and
prints the same tables in the log. Questions:
  1. measurements: which types, units and value ranges; on which event types?
  2. descent milestones of the arrivals: top of descent, last FL100 crossing, arrival runway, landing --
     coverage per airport, distances to the airport, durations to the runway;
  3. level segments after the top of descent: how do level-start / level-end pair, at which flight levels,
     how long, and do some look like holding patterns (much more distance flown than displacement)?
  4. a few complete arrival sequences, to read the events by eye.
Each part is independent: an error in one is recorded in the report and the others still run.
"""
import traceback

from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import Steps, log, write_report

FOCUS = ["LFPG", "LFBO", "ENZV"]
N_TOP = 10                       # + the N busiest destinations of the sample
LEVEL_TYPES = ["level-start", "level-end"]
MILESTONE_TYPES = ["top-of-descent", "last-xing-fl245", "last-xing-fl100", "last-xing-fl70", "last-xing-fl50",
                   "landing", "entry-runway", "exit-runway", "last_seen"]
PCTS = [0.1, 0.25, 0.5, 0.75, 0.9]


def km_between(lat1, lon1, lat2, lon2):
    """Great-circle distance (km) between two positions (Spark columns)."""
    a = (F.sin(F.radians(lat2 - lat1) / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lon2 - lon1) / 2) ** 2)
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def quantiles(df, column, label=None):
    """{p10, p25, p50, p75, p90, n} of one numeric column."""
    row = df.agg(F.count(column).alias("n"),
                 F.percentile_approx(column, PCTS, 1000).alias("q")).first()
    out = {"n": row["n"]}
    if row["q"]:
        out.update({f"p{int(p * 100)}": round(v, 2) for p, v in zip(PCTS, row["q"])})
    return out if label is None else {label: out}


def rows(df, n=60):
    return [r.asDict(recursive=True) for r in df.limit(n).collect()]


def show(title, data):
    log.info("---- %s", title)
    if isinstance(data, list):
        for r in data:
            log.info("   %s", r)
    else:
        log.info("   %s", data)


# --------------------------------------------------------------------------------------------- 1. measurements
def explore_measurements(spark, settings, events):
    m = spark.read.parquet(str(settings.bronze_dir / "measurements")).select("event_id", "type", "value")
    by_type = (m.groupBy("type")
               .agg(F.count("*").alias("rows"), F.min("value").alias("min"),
                    F.percentile_approx("value", [0.5, 0.9], 1000).alias("p50_p90"), F.max("value").alias("max"))
               .orderBy(F.desc("rows")))
    on_events = (m.join(events.select("event_id", "event_type"), "event_id")
                 .groupBy("event_type", "type").count().orderBy("event_type", "type"))
    per_event = m.groupBy("event_id").count().groupBy(F.col("count").alias("measurements_per_event")).count()
    out = {"by_type": rows(by_type), "by_event_type": rows(on_events, 200),
           "measurements_per_event": rows(per_event.orderBy("measurements_per_event"))}
    for k, v in out.items():
        show(f"measurements {k}", v)
    return out, m


# --------------------------------------------------------------------------------------------- 2. milestones
def arrivals_frame(flights, events):
    """Events of the flights with a known destination, with the destination position."""
    arr = (flights.filter(F.col("dest_icao").isNotNull() & F.col("dest_lat").isNotNull())
           .select("flight_id", "dest_icao", "dest_lat", "dest_lon", "aircraft_class", "aircraft_typecode",
                   "operator_icao"))
    return arr, events.join(arr.select("flight_id", "dest_icao", "dest_lat", "dest_lon"), "flight_id")


def milestones(ev):
    """One row per arrival: time / altitude / distance to the airport of each descent milestone."""
    ev = ev.withColumn("dist_km", km_between(F.col("latitude"), F.col("longitude"), F.col("dest_lat"),
                                             F.col("dest_lon")))
    at_dest = (F.col("info").getItem("osm_airport") == F.col("dest_icao"))
    keep = (F.col("event_type").isin("top-of-descent", "last-xing-fl245", "last-xing-fl100", "last-xing-fl70",
                                     "last-xing-fl50", "last_seen", "landing")
            | (F.col("event_type").isin("entry-runway", "exit-runway") & at_dest))
    sel = ev.filter(keep)
    aggs = []
    for t in MILESTONE_TYPES:
        name = t.replace("-", "_")
        s = F.struct(F.col("event_timestamp_utc").alias("t"), F.col("altitude_ft").alias("alt"),
                     F.col("dist_km").alias("km"), F.col("event_id").alias("event_id"),
                     F.col("info").getItem("osm_ref").alias("ref"))
        # last occurrence for descent events; first runway entry at the destination for runway events
        agg = F.min if t in ("entry-runway", "exit-runway") else F.max
        aggs += [agg(F.when(F.col("event_type") == t, s)).alias(name),
                 F.sum((F.col("event_type") == t).cast("int")).alias(f"n_{name}")]
    return sel.groupBy("flight_id", "dest_icao").agg(*aggs)


def explore_milestones(ms, focus):
    m = ms
    minutes = lambda a, b: (F.unix_timestamp(f"{b}.t") - F.unix_timestamp(f"{a}.t")) / 60.0
    m = (m.withColumn("tod_to_rwy_min", minutes("top_of_descent", "entry_runway"))
          .withColumn("fl100_to_rwy_min", minutes("last_xing_fl100", "entry_runway"))
          .withColumn("tod_to_landing_min", minutes("top_of_descent", "landing"))
          .withColumn("rwy_after_landing_s", F.unix_timestamp("entry_runway.t") - F.unix_timestamp("landing.t")))
    coverage = (m.groupBy("dest_icao").agg(
        F.count("*").alias("arrivals"),
        F.round(F.avg(F.col("top_of_descent").isNotNull().cast("int")) * 100, 1).alias("pct_tod"),
        F.round(F.avg(F.col("last_xing_fl100").isNotNull().cast("int")) * 100, 1).alias("pct_fl100"),
        F.round(F.avg(F.col("entry_runway").isNotNull().cast("int")) * 100, 1).alias("pct_rwy"),
        F.round(F.avg(F.col("landing").isNotNull().cast("int")) * 100, 1).alias("pct_landing"),
        F.round(F.avg((F.col("top_of_descent").isNotNull() & F.col("entry_runway").isNotNull()).cast("int")) * 100,
                1).alias("pct_tod_and_rwy"),
        F.round(F.avg("n_top_of_descent"), 2).alias("avg_tod_events"),
        F.percentile_approx("top_of_descent.alt", 0.5).alias("med_tod_alt_ft"),
        F.round(F.percentile_approx("top_of_descent.km", 0.5), 0).alias("med_tod_km"),
        F.round(F.percentile_approx("last_xing_fl100.km", 0.5), 0).alias("med_fl100_km"),
        F.round(F.percentile_approx("tod_to_rwy_min", 0.5), 1).alias("med_tod_to_rwy_min"),
        F.round(F.percentile_approx("fl100_to_rwy_min", 0.5), 1).alias("med_fl100_to_rwy_min"))
        .orderBy(F.desc("arrivals")))
    top = [r["dest_icao"] for r in coverage.limit(N_TOP).collect()]
    airports = list(dict.fromkeys(focus + top))
    out = {
        "airports": airports,
        "coverage": rows(coverage.filter(F.col("dest_icao").isin(airports)), 40),
        "tod_events_per_arrival": rows(m.groupBy("n_top_of_descent").count().orderBy("n_top_of_descent"), 10),
        "tod_altitude_ft": quantiles(m, "top_of_descent.alt"),
        "tod_distance_km": quantiles(m, "top_of_descent.km"),
        "fl100_distance_km": quantiles(m, "last_xing_fl100.km"),
        "fl70_distance_km": quantiles(m, "last_xing_fl70.km"),
        "tod_to_runway_min": quantiles(m.filter("tod_to_rwy_min BETWEEN 0 AND 180"), "tod_to_rwy_min"),
        "fl100_to_runway_min": quantiles(m.filter("fl100_to_rwy_min BETWEEN 0 AND 120"), "fl100_to_rwy_min"),
        "runway_entry_minus_landing_s": quantiles(m, "rwy_after_landing_s"),
        "negative_or_huge_tod_to_runway": m.filter("tod_to_rwy_min < 0 OR tod_to_rwy_min > 180").count(),
        "runway_ref_examples": rows(m.filter(F.col("dest_icao").isin(FOCUS))
                                    .groupBy("dest_icao", F.col("entry_runway.ref").alias("ref")).count()
                                    .orderBy("dest_icao", F.desc("count")), 30),
    }
    for k, v in out.items():
        show(f"milestones {k}", v)
    return out, m


# --------------------------------------------------------------------------------------------- 3. level segments
def explore_levels(ev, ms, meas, airports):
    """Level events after the top of descent of each arrival, paired with the next level event."""
    tod = ms.select("flight_id", F.col("top_of_descent.t").alias("tod_t"), F.col("entry_runway.t").alias("rwy_t"),
                    F.col("landing.t").alias("landing_t"))
    w = Window.partitionBy("flight_id").orderBy("event_timestamp_utc", "event_id")
    lv_all = ev.filter(F.col("event_type").isin(LEVEL_TYPES))
    seq = (lv_all.withColumn("next_type", F.lead("event_type").over(w))
           .withColumn("next_t", F.lead("event_timestamp_utc").over(w))
           .withColumn("next_alt", F.lead("altitude_ft").over(w))
           .withColumn("next_lat", F.lead("latitude").over(w))
           .withColumn("next_lon", F.lead("longitude").over(w))
           .withColumn("next_event_id", F.lead("event_id").over(w))
           .join(tod, "flight_id"))
    out = {}
    # pairing, before and after the top of descent
    phase = F.when(F.col("tod_t").isNull(), "no_tod").when(F.col("event_timestamp_utc") < F.col("tod_t"),
                                                            "before_tod").otherwise("after_tod")
    out["next_event_after_level_type"] = rows(
        seq.withColumn("phase", phase).groupBy("phase", "event_type", "next_type").count()
        .orderBy("phase", "event_type", F.desc("count")), 40)
    starts = (seq.filter((F.col("event_type") == "level-start") & (F.col("event_timestamp_utc") >= F.col("tod_t")))
              .withColumn("paired", (F.col("next_type") == "level-end")
                          & (F.abs(F.col("next_alt") - F.col("altitude_ft")) <= 300))
              .withColumn("dur_min", (F.unix_timestamp("next_t") - F.unix_timestamp("event_timestamp_utc")) / 60.0)
              .withColumn("displacement_km", km_between(F.col("latitude"), F.col("longitude"), F.col("next_lat"),
                                                        F.col("next_lon")))
              .withColumn("band", F.when(F.col("altitude_ft") < 7000, "a_<FL70")
                          .when(F.col("altitude_ft") < 10000, "b_FL70-100")
                          .when(F.col("altitude_ft") < 20000, "c_FL100-200")
                          .when(F.col("altitude_ft") < 30000, "d_FL200-300").otherwise("e_>=FL300")))
    out["level_starts_after_tod"] = starts.count()
    out["pct_paired_with_level_end"] = round(starts.agg(F.avg(F.col("paired").cast("int"))).first()[0] * 100, 1) \
        if out["level_starts_after_tod"] else None
    paired = starts.filter("paired AND dur_min >= 0")
    out["paired_duration_min"] = quantiles(paired, "dur_min")
    out["paired_altitude_ft"] = quantiles(paired, "altitude_ft")
    out["paired_by_band"] = rows(paired.groupBy("band").agg(
        F.count("*").alias("segments"), F.round(F.percentile_approx("dur_min", 0.5), 2).alias("med_min"),
        F.round(F.percentile_approx("dur_min", 0.9), 2).alias("p90_min"),
        F.round(F.sum("dur_min") / 60, 1).alias("total_hours")).orderBy("band"))
    per_flight = (starts.groupBy("flight_id").agg(F.count("*").alias("n"), F.sum(F.when(F.col("paired"),
                                                                                         F.col("dur_min"))).alias("min")))
    arrivals_with_tod = ms.filter(F.col("top_of_descent").isNotNull()).select("flight_id", "dest_icao")
    pf = arrivals_with_tod.join(per_flight, "flight_id", "left").fillna(0, ["n", "min"])
    out["level_segments_per_arrival"] = quantiles(pf, "n")
    out["level_minutes_per_arrival"] = quantiles(pf, "min")
    out["by_airport"] = rows(pf.filter(F.col("dest_icao").isin(airports)).groupBy("dest_icao").agg(
        F.count("*").alias("arrivals_with_tod"), F.round(F.avg("n"), 2).alias("avg_levels"),
        F.round(F.avg("min"), 2).alias("avg_level_min"),
        F.round(F.avg((F.col("min") >= 4).cast("int")) * 100, 1).alias("pct_ge_4min_level"))
        .orderBy(F.desc("arrivals_with_tod")), 40)

    # holding signature: long level segment, distance flown >> displacement (needs a distance measurement)
    dist_types = [r["type"] for r in meas.select("type").distinct().collect() if "dist" in r["type"].lower()]
    out["distance_measurement_type"] = dist_types
    long_seg = paired.filter("dur_min >= 3")
    if dist_types:
        d = meas.filter(F.col("type") == dist_types[0]).select("event_id", F.col("value").alias("d"))
        long_seg = (long_seg.join(d.withColumnRenamed("d", "d_start"), "event_id", "left")
                    .join(d.withColumnRenamed("event_id", "next_event_id").withColumnRenamed("d", "d_end"),
                          "next_event_id", "left")
                    .withColumn("flown", F.col("d_end") - F.col("d_start"))
                    .withColumn("ratio_flown_displacement", F.col("flown") / F.greatest(F.col("displacement_km"),
                                                                                        F.lit(0.1))))
        out["long_segment_flown"] = quantiles(long_seg, "flown")
        out["long_segment_ratio_flown_displacement"] = quantiles(long_seg, "ratio_flown_displacement")
    out["long_segment_displacement_km"] = quantiles(long_seg, "displacement_km")
    out["long_segments_examples"] = rows(long_seg.filter(F.col("dest_icao").isin(airports))
                                         .orderBy(F.desc("dur_min"))
                                         .select("flight_id", "altitude_ft", "dur_min", "displacement_km",
                                                 *(["flown", "ratio_flown_displacement"] if dist_types else [])), 15)
    for k, v in out.items():
        show(f"levels {k}", v)
    return out


# --------------------------------------------------------------------------------------------- 4. sequences
def explore_sequences(ev, ms, meas):
    """Complete event sequences of a few arrivals (the ones with the most level segments after the descent)."""
    candidates = (ms.filter(F.col("dest_icao").isin(FOCUS) & F.col("top_of_descent").isNotNull()
                            & F.col("entry_runway").isNotNull())
                  .select("flight_id", "dest_icao").limit(2000))
    nlev = ev.filter(F.col("event_type") == "level-start").groupBy("flight_id").count()
    picked = (candidates.join(nlev, "flight_id").withColumn("rk", F.row_number().over(
        Window.partitionBy("dest_icao").orderBy(F.desc("count")))).filter("rk <= 2").select("flight_id")
        .collect())
    ids = [r["flight_id"] for r in picked]
    if not ids:
        return {"flights": []}
    m = meas.filter(F.col("event_id").isin([r["event_id"] for r in ev.filter(F.col("flight_id").isin(ids))
                                            .select("event_id").collect()]))
    mp = m.groupBy("event_id").agg(F.map_from_entries(F.collect_list(F.struct("type", "value"))).alias("m"))
    seq = (ev.filter(F.col("flight_id").isin(ids)).join(mp, "event_id", "left")
           .select("flight_id", "dest_icao", "event_timestamp_utc", "event_type", "altitude_ft",
                   F.round("latitude", 3).alias("lat"), F.round("longitude", 3).alias("lon"),
                   F.col("info").getItem("osm_airport").alias("apt"), F.col("info").getItem("osm_ref").alias("ref"),
                   "m")
           .orderBy("flight_id", "event_timestamp_utc"))
    out = {"flights": ids, "events": rows(seq, 400)}
    show("sequences", out["events"])
    return out


# --------------------------------------------------------------------------------------------- run
def run(spark, settings):
    steps = Steps()
    report = {"errors": {}}
    flights = spark.read.parquet(str(settings.silver_dir / "flights"))
    events = spark.read.parquet(str(settings.silver_dir / "flight_events"))
    arr, ev = arrivals_frame(flights, events)
    ev = ev.persist()
    report["sample"] = {"flights": flights.count(), "events": events.count(), "arrivals": arr.count()}
    show("sample", report["sample"])

    meas = ms = None
    parts = [("measurements", lambda: explore_measurements(spark, settings, events)),
             ("milestones", lambda: explore_milestones(milestones(ev).persist(), FOCUS))]
    for name, fn in parts:
        try:
            with steps.step(name):
                report[name], obj = fn()
            if name == "measurements":
                meas = obj
            else:
                ms = obj
        except Exception:
            report["errors"][name] = traceback.format_exc()
            log.error("explore %s failed:\n%s", name, report["errors"][name])
    if ms is not None and meas is not None:
        for name, fn in [("levels", lambda: explore_levels(ev, ms, meas, report["milestones"]["airports"])),
                         ("sequences", lambda: explore_sequences(ev, ms, meas))]:
            try:
                with steps.step(name):
                    report[name] = fn()
            except Exception:
                report["errors"][name] = traceback.format_exc()
                log.error("explore %s failed:\n%s", name, report["errors"][name])
    report["durations_s"] = steps.durations
    write_report(settings, "explore", report)
    log.info("Exploration report: %s", settings.report_dir / "explore.json")
    return report
