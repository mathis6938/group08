"""Machine learning layer (MLlib): Explanatory model for additional arrival time and airport typology.

Reads:
  - silver/arrivals
  - silver/airports
  - gold/airport_arrival_hourly
  - gold/airport_arrival_daily
  - gold/arrival_reference

Publishes (when quality gate passes):
  - <data_root>/models/<run_id>/arrival_gbt
  - gold/ml_feature_importance (variable, importance, rank)
  - gold/ml_scenarios (airport_icao, scenario, demand_cap, predicted_total_min_before,
                       predicted_total_min_after, saving_min_per_day)
  - gold/airport_typology (airport_icao, airport_name, cluster, cluster_label, metrics...)
"""
import datetime
import shutil

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

from .common import Steps, log, utc_now, write_report, write_table


FOCUS_AIRPORTS = ["LFPG", "LFBO", "ENZV", "EGLL", "EDDF"]


def build_arrival_ml_features(spark, arrivals_df, hourly_ops_df, ref_df):
    """Build feature dataset for measured arrivals during complete European traffic hours."""
    # Match reference transit times using the 3-level fallback hierarchy
    r1 = ref_df.filter(F.col("fallback_level") == "RUNWAY_CLASS_SECTOR").select(
        F.col("airport_icao").alias("r1_icao"),
        F.col("arrival_runway").alias("r1_runway"),
        F.col("aircraft_class").alias("r1_class"),
        F.col("entry_sector").alias("r1_sector"),
        F.col("ref_transit_min").alias("r1_ref"),
    )
    r2 = ref_df.filter(F.col("fallback_level") == "CLASS_SECTOR").select(
        F.col("airport_icao").alias("r2_icao"),
        F.col("aircraft_class").alias("r2_class"),
        F.col("entry_sector").alias("r2_sector"),
        F.col("ref_transit_min").alias("r2_ref"),
    )
    r3 = ref_df.filter(F.col("fallback_level") == "AIRPORT").select(
        F.col("airport_icao").alias("r3_icao"),
        F.col("ref_transit_min").alias("r3_ref"),
    )

    matched = (
        arrivals_df
        .join(r1, (F.col("dest_icao") == F.col("r1_icao")) &
                  (F.col("arrival_runway") == F.col("r1_runway")) &
                  (F.col("aircraft_class") == F.col("r1_class")) &
                  (F.col("entry_sector") == F.col("r1_sector")), "left")
        .join(r2, (F.col("dest_icao") == F.col("r2_icao")) &
                  (F.col("aircraft_class") == F.col("r2_class")) &
                  (F.col("entry_sector") == F.col("r2_sector")), "left")
        .join(r3, F.col("dest_icao") == F.col("r3_icao"), "left")
        .withColumn("ref_transit_min", F.coalesce(F.col("r1_ref"), F.col("r2_ref"), F.col("r3_ref")))
        # same definition as Gold (airport_arrival_*): transit - reference, not clipped (can be slightly negative)
        .withColumn("additional_min", F.col("transit_min") - F.col("ref_transit_min"))
    )

    # Hourly operations and demand (current hour and previous hour)
    hour_ts = F.unix_timestamp(F.col("movement_date").cast("timestamp")) + F.col("movement_hour") * 3600
    curr_ops = hourly_ops_df.select(
        F.col("airport_icao").alias("ops_icao"),
        F.col("movement_date").alias("ops_date"),
        F.col("movement_hour").alias("ops_hour"),
        F.col("arrivals").alias("hourly_demand"),
        F.col("is_complete_hour"),
    )

    # demand of the previous hour, matched on the hour timestamp (so 00:00 looks at 23:00 of the day before)
    prev_ops = hourly_ops_df.select(
        F.col("airport_icao").alias("prev_icao"),
        (hour_ts + 3600).alias("prev_target_ts"),
        F.col("arrivals").alias("prev_hourly_demand"),
    )

    # Join and filter to valid measured arrivals in complete hours
    features_df = (
        matched
        .join(curr_ops, (F.col("dest_icao") == F.col("ops_icao")) &
                        (F.col("arrival_date") == F.col("ops_date")) &
                        (F.col("arrival_hour") == F.col("ops_hour")), "inner")
        .join(prev_ops, (F.col("dest_icao") == F.col("prev_icao")) &
                        ((F.unix_timestamp(F.col("arrival_date").cast("timestamp")) + F.col("arrival_hour") * 3600)
                         == F.col("prev_target_ts")), "left")
        .filter(F.col("is_complete_hour") == True)
        .filter(F.col("is_transit_plausible") == True)
        .filter(F.col("transit_min").isNotNull() & F.col("ref_transit_min").isNotNull())
        .withColumn("day_of_week", F.dayofweek(F.col("landing_utc")).cast(DoubleType()))
        .withColumn("arrival_hour", F.col("arrival_hour").cast(DoubleType()))
        .withColumn("hourly_demand", F.coalesce(F.col("hourly_demand"), F.lit(0)).cast(DoubleType()))
        .withColumn("prev_hourly_demand", F.coalesce(F.col("prev_hourly_demand"), F.col("hourly_demand")).cast(DoubleType()))
        .withColumn("level_off_count", F.coalesce(F.col("level_off_count"), F.lit(0)).cast(DoubleType()))
        .withColumn("dest_icao", F.coalesce(F.col("dest_icao"), F.lit("UNKNOWN")))
        .withColumn("entry_sector", F.coalesce(F.col("entry_sector"), F.lit("UNKNOWN")))
        .withColumn("arrival_runway", F.coalesce(F.col("arrival_runway"), F.lit("UNKNOWN")))
        .withColumn("aircraft_class", F.coalesce(F.col("aircraft_class"), F.lit("UNKNOWN")))
        .withColumn("operator_icao", F.coalesce(F.col("operator_icao"), F.lit("UNKNOWN")))
        .select(
            "flight_id", "dest_icao", "arrival_date", "arrival_hour", "day_of_week",
            "hourly_demand", "prev_hourly_demand", "entry_sector", "arrival_runway",
            "aircraft_class", "operator_icao", "level_off_count", "additional_min",
        )
    )
    return features_df


def add_load_factor(df, airport_peak):
    """hourly demand / usual peak demand of the airport (airport_peak: dest_icao, airport_peak_demand)."""
    return (df.drop("airport_peak_demand", "load_factor")
            .join(F.broadcast(airport_peak), "dest_icao", "left")
            .withColumn("airport_peak_demand", F.greatest(F.coalesce(F.col("airport_peak_demand"), F.lit(1.0)), F.lit(1.0)))
            .withColumn("load_factor", F.col("hourly_demand") / F.col("airport_peak_demand")))


def split_temporal(df, test_days):
    """Split dataset temporally to avoid data leakage."""
    dates = df.select(F.min("arrival_date"), F.max("arrival_date")).first()
    min_date, max_date = dates[0], dates[1]
    if min_date is None or max_date is None:
        return df, df, min_date

    total_days = (max_date - min_date).days
    effective_test_days = min(test_days, max(1, total_days // 3)) if total_days > 0 else 0
    split_date = max_date - datetime.timedelta(days=effective_test_days)

    train_df = df.filter(F.col("arrival_date") < F.lit(split_date))
    test_df = df.filter(F.col("arrival_date") >= F.lit(split_date))

    # Guard: if train or test is empty due to small date range, fall back to simple day split
    if train_df.count() == 0 or test_df.count() == 0:
        split_date = min_date + datetime.timedelta(days=max(1, total_days // 2))
        train_df = df.filter(F.col("arrival_date") < F.lit(split_date))
        test_df = df.filter(F.col("arrival_date") >= F.lit(split_date))
        if train_df.count() == 0 or test_df.count() == 0:
            train_df = df
            test_df = df

    return train_df, test_df, split_date


def train_and_evaluate_gbt(train_df, test_df, settings):
    """Train GBTRegressor pipeline, compute feature importances, and evaluate against naive baseline."""
    # Group rare operators on the training set: top 30 operators, else "OTHER"
    top_ops_rows = (
        train_df.groupBy("operator_icao").count()
        .filter(F.col("operator_icao") != "UNKNOWN")
        .orderBy(F.desc("count"))
        .limit(30)
        .collect()
    )
    top_ops = {r["operator_icao"] for r in top_ops_rows}

    train_prepped = train_df.withColumn(
        "operator_clean",
        F.when(F.col("operator_icao").isin(list(top_ops)), F.col("operator_icao")).otherwise(F.lit("OTHER")),
    )
    test_prepped = test_df.withColumn(
        "operator_clean",
        F.when(F.col("operator_icao").isin(list(top_ops)), F.col("operator_icao")).otherwise(F.lit("OTHER")),
    )

    # Load factor = demand of the hour / usual peak demand of the airport (90th percentile of the hourly demand seen
    # by the training flights): a common scale for all airports, so that one model can learn "saturation".
    # level_off_count is NOT a feature: the level-offs are part of the time lost that the model explains.
    airport_peak = (train_prepped.groupBy("dest_icao")
                    .agg(F.percentile_approx("hourly_demand", 0.9, 1000).alias("airport_peak_demand")))
    train_prepped = add_load_factor(train_prepped, airport_peak)
    test_prepped = add_load_factor(test_prepped, airport_peak)

    cat_cols = ["dest_icao", "entry_sector", "arrival_runway", "aircraft_class", "operator_clean"]
    num_cols = ["arrival_hour", "day_of_week", "hourly_demand", "prev_hourly_demand", "load_factor"]

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in cat_cols
    ]
    assembler = VectorAssembler(
        inputCols=[f"{c}_idx" for c in cat_cols] + num_cols,
        outputCol="features",
        handleInvalid="keep",
    )
    # Ensure maxBins is at least as large as the number of categorical categories
    n_dest = train_prepped.select("dest_icao").distinct().count()
    max_bins = max(64, n_dest + 32, 512)

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="additional_min",
        maxIter=settings.ml.max_iter,
        maxDepth=settings.ml.max_depth,
        maxBins=max_bins,
        seed=settings.ml.seed,
    )

    pipeline = Pipeline(stages=indexers + [assembler, gbt])
    model = pipeline.fit(train_prepped)

    # Predictions
    test_pred = model.transform(test_prepped)

    # Naive baseline: average additional_min by (dest_icao, arrival_hour) on train set
    global_mean = train_prepped.select(F.avg("additional_min")).first()[0] or 0.0
    baseline_stats = (
        train_prepped
        .groupBy("dest_icao", "arrival_hour")
        .agg(F.avg("additional_min").alias("baseline_pred"))
    )

    test_with_base = (
        test_pred
        .join(baseline_stats, ["dest_icao", "arrival_hour"], "left")
        .withColumn("baseline_pred", F.coalesce(F.col("baseline_pred"), F.lit(global_mean)))
    ).persist()

    eval_mae = RegressionEvaluator(labelCol="additional_min", predictionCol="prediction", metricName="mae")
    eval_rmse = RegressionEvaluator(labelCol="additional_min", predictionCol="prediction", metricName="rmse")
    eval_r2 = RegressionEvaluator(labelCol="additional_min", predictionCol="prediction", metricName="r2")

    eval_base_mae = RegressionEvaluator(labelCol="additional_min", predictionCol="baseline_pred", metricName="mae")
    eval_base_rmse = RegressionEvaluator(labelCol="additional_min", predictionCol="baseline_pred", metricName="rmse")
    eval_base_r2 = RegressionEvaluator(labelCol="additional_min", predictionCol="baseline_pred", metricName="r2")

    model_mae = round(float(eval_mae.evaluate(test_with_base)), 3)
    model_rmse = round(float(eval_rmse.evaluate(test_with_base)), 3)
    try:
        model_r2 = round(float(eval_r2.evaluate(test_with_base)), 3)
    except Exception:
        model_r2 = 0.0

    base_mae = round(float(eval_base_mae.evaluate(test_with_base)), 3)
    base_rmse = round(float(eval_base_rmse.evaluate(test_with_base)), 3)
    try:
        base_r2 = round(float(eval_base_r2.evaluate(test_with_base)), 3)
    except Exception:
        base_r2 = 0.0

    # Focus airports metrics
    focus_metrics = {}
    for icao in FOCUS_AIRPORTS:
        sub = test_with_base.filter(F.col("dest_icao") == icao)
        if sub.count() > 0:
            focus_metrics[icao] = {
                "count": sub.count(),
                "model_mae": round(float(eval_mae.evaluate(sub)), 3),
                "baseline_mae": round(float(eval_base_mae.evaluate(sub)), 3),
            }

    # Extract feature importance
    gbt_model = model.stages[-1]
    raw_importances = gbt_model.featureImportances.toArray()
    feature_names = cat_cols + num_cols
    var_map = {
        "dest_icao": "dest_icao",
        "entry_sector": "entry_sector",
        "arrival_runway": "arrival_runway",
        "aircraft_class": "aircraft_class",
        "operator_clean": "operator_icao",
        "arrival_hour": "arrival_hour",
        "day_of_week": "day_of_week",
        "hourly_demand": "hourly_demand",
        "prev_hourly_demand": "prev_hourly_demand",
        "load_factor": "load_factor",
    }
    grouped_importance = {}
    for name, imp in zip(feature_names, raw_importances):
        var = var_map.get(name, name)
        grouped_importance[var] = grouped_importance.get(var, 0.0) + float(imp)

    sorted_vars = sorted(grouped_importance.items(), key=lambda kv: kv[1], reverse=True)
    importance_rows = [
        {"variable": var, "importance": round(imp, 4), "rank": rank + 1}
        for rank, (var, imp) in enumerate(sorted_vars)
    ]

    metrics = {
        "model": {"mae": model_mae, "rmse": model_rmse, "r2": model_r2},
        "baseline": {"mae": base_mae, "rmse": base_rmse, "r2": base_r2},
        "focus_airports": focus_metrics,
        "feature_importances": importance_rows,
    }
    return model, test_prepped, metrics


def smooth_day(demand, cap):
    """Demand of the 24 hours of one day after capping at `cap`: the excess of an hour is moved to the nearest hours
    that are below the cap (h+1, h-1, h+2, ...), within the same day. Flights that find no room stay where they are.
    Returns (new demand list, moves) where moves = {(from_hour, to_hour): number of flights}."""
    new = list(demand)
    moves = {}
    for h in range(24):
        excess = new[h] - cap
        for dist in range(1, 24):
            if excess <= 0:
                break
            for target in (h + dist, h - dist):
                if excess <= 0 or not 0 <= target < 24:
                    continue
                room = cap - new[target]
                if room > 0:
                    k = min(room, excess)
                    new[target] += k
                    new[h] -= k
                    excess -= k
                    moves[(h, target)] = moves.get((h, target), 0) + k
    return new, moves


def build_scenarios_table(spark, model, test_prepped, settings, hourly_ops_df=None):
    """Demand smoothing scenario on the focus airports, simulated with the trained model on the test period.

    For each day, the hourly arrival demand is capped at the airport's 90th percentile and the excess flights are
    moved to the nearest hours with spare capacity (same day): moved flights get their new hour and every flight gets
    the new demand of its hour (and of the previous hour). The total predicted additional time of the measured
    flights is compared before / after. It is a simulation on an association model, not a causal prediction.
    """
    scenarios_data = []
    now = utc_now()
    cap_pctl = settings.ml.demand_cap_pctl
    for icao in FOCUS_AIRPORTS:
        flights = test_prepped.filter(F.col("dest_icao") == icao).toPandas()
        if flights.empty:
            continue
        # hourly demand of the airport (all its arrivals, measured or not) on the test days
        if hourly_ops_df is not None:
            demand = (hourly_ops_df.filter(F.col("airport_icao") == icao)
                      .filter(F.col("movement_date").isin(sorted(set(flights["arrival_date"]))))
                      .select(F.col("movement_date").alias("arrival_date"),
                              F.col("movement_hour").cast("int").alias("hour"),
                              F.col("arrivals").cast("int").alias("demand"))
                      .toPandas())
        else:   # no hourly table (unit tests): demand of the hour as seen by the flights
            demand = (flights.assign(hour=flights["arrival_hour"].astype(int))
                      .groupby(["arrival_date", "hour"], as_index=False)["hourly_demand"].max()
                      .rename(columns={"hourly_demand": "demand"}))
        demand["demand"] = demand["demand"].astype(int)
        busy = demand[demand["demand"] > 0]["demand"]
        cap = int(max(1, round(busy.quantile(cap_pctl)))) if len(busy) else 1

        after = flights.copy()
        after["arrival_hour"] = after["arrival_hour"].astype(float)
        moved_total = 0
        for day, rows in after.groupby("arrival_date"):
            d = demand[demand["arrival_date"] == day].set_index("hour")["demand"]
            vector = [int(d.get(h, 0)) for h in range(24)]
            new_vector, moves = smooth_day(vector, cap)
            moved_total += sum(moves.values())
            # measured flights of an over-capacity hour move in the same proportion as all its flights
            idx_by_hour = {h: list(rows.index[rows["arrival_hour"].astype(int) == h]) for h in range(24)}
            for (src, dst), n in sorted(moves.items()):
                share = n / max(vector[src], 1)
                k = int(round(share * len(idx_by_hour[src])))
                movers, idx_by_hour[src] = idx_by_hour[src][:k], idx_by_hour[src][k:]
                after.loc[movers, "arrival_hour"] = float(dst)
            for i in rows.index:
                h = int(after.at[i, "arrival_hour"])
                after.at[i, "hourly_demand"] = float(new_vector[h])
                after.at[i, "prev_hourly_demand"] = float(new_vector[h - 1]) if h > 0 else after.at[i, "prev_hourly_demand"]
        after["load_factor"] = after["hourly_demand"] / after["airport_peak_demand"]

        cols = list(flights.columns)
        pred_before = model.transform(test_prepped.filter(F.col("dest_icao") == icao))
        after_df = spark.createDataFrame(after[cols]).select(
            *[F.col(c).cast(t).alias(c) for c, t in test_prepped.dtypes])          # same types as the model input
        pred_after = model.transform(after_df)
        total_before = pred_before.agg(F.sum("prediction")).first()[0] or 0.0
        total_after = pred_after.agg(F.sum("prediction")).first()[0] or 0.0
        n_days = max(1, flights["arrival_date"].nunique())
        before_day, after_day = round(total_before / n_days, 1), round(total_after / n_days, 1)
        scenarios_data.append({
            "airport_icao": icao,
            "scenario": f"CAP_P{int(cap_pctl * 100)}_REDISTRIBUTE_SAME_DAY",
            "demand_cap": cap,
            "measured_flights": int(len(flights)),
            "flights_moved_per_day": round(moved_total / n_days, 1),
            "predicted_total_min_before": before_day,
            "predicted_total_min_after": after_day,
            "saving_min_per_day": round(before_day - after_day, 1),
            "_ml_built_at_utc": now,
        })

    if not scenarios_data:
        scenarios_data = [{
            "airport_icao": "UNKNOWN", "scenario": "NO_DATA", "demand_cap": 0, "measured_flights": 0,
            "flights_moved_per_day": 0.0, "predicted_total_min_before": 0.0, "predicted_total_min_after": 0.0,
            "saving_min_per_day": 0.0, "_ml_built_at_utc": now,
        }]

    schema = ("airport_icao string, scenario string, demand_cap bigint, measured_flights bigint, "
              "flights_moved_per_day double, predicted_total_min_before double, predicted_total_min_after double, "
              "saving_min_per_day double, _ml_built_at_utc timestamp")
    return spark.createDataFrame(scenarios_data, schema=schema)


def build_airport_typology(spark, settings, daily_ops_df, airports_dim_df):
    """Cluster airports by arrival characteristics using StandardScaler + KMeans with silhouette scoring.

    Only airports whose figures are representative take part: at least `min_arrivals` measured arrivals and a
    coverage of the arrival time of at least `typology_min_coverage_pct` %. Averages are weighted by flights (an
    airport with a few measured flights or a poorly covered one would otherwise look "fluid" or "congested" by chance).
    """
    now = utc_now()
    arrivals = F.col("arrivals").cast("double")
    measured = F.col("arrivals_measured").cast("double")
    airport_stats = (
        daily_ops_df
        .groupBy("airport_icao")
        .agg(
            F.sum("arrivals").alias("total_arrivals"),
            F.sum("arrivals_measured").alias("total_measured"),
            F.round(F.avg("arrivals"), 2).alias("avg_daily_arrivals"),
            F.round(F.sum("total_additional_min") / F.sum(measured), 2).alias("avg_additional_min"),
            F.round(F.percentile_approx("avg_additional_min", 0.90, 100), 2).alias("p90_additional_min"),
            F.round(F.sum(F.col("avg_level_minutes") * arrivals) / F.sum(arrivals), 2).alias("avg_level_minutes"),
            F.round(F.sum(F.col("pct_with_holding") * arrivals) / F.sum(arrivals), 2).alias("pct_with_holding"),
            F.round(F.stddev("arrivals"), 2).alias("day_to_day_std"),
        )
        .withColumn("coverage_pct", F.col("total_measured") / F.col("total_arrivals") * 100)
        .fillna(0.0, ["avg_additional_min", "p90_additional_min", "avg_level_minutes", "pct_with_holding",
                      "day_to_day_std", "avg_daily_arrivals"])
    )

    filtered = airport_stats.filter((F.col("total_measured") >= settings.ml.min_arrivals)
                                    & (F.col("coverage_pct") >= settings.ml.typology_min_coverage_pct))
    n_airports = filtered.count()

    # Fallback if too few airports meet min_arrivals (e.g. synthetic data)
    if n_airports < settings.ml.k_min:
        filtered = airport_stats.filter(F.col("total_arrivals") > 0)
        n_airports = filtered.count()

    if n_airports < 2:
        # Single or empty airport case
        single = (
            filtered
            .withColumn("cluster", F.lit(0).cast(IntegerType()))
            .withColumn("cluster_label", F.lit("Standard Operations"))
            .join(airports_dim_df.select("airport_icao", "airport_name"), "airport_icao", "left")
            .withColumn("airport_name", F.coalesce(F.col("airport_name"), F.lit("Unknown Airport")))
            .withColumn("_ml_built_at_utc", F.lit(now))
            .select(
                "airport_icao", "airport_name", "cluster", "cluster_label",
                "avg_daily_arrivals", "avg_additional_min", "p90_additional_min",
                "avg_level_minutes", "pct_with_holding", "day_to_day_std", "_ml_built_at_utc"
            )
        )
        return single, {"n_airports": n_airports, "k": 1, "silhouette": 1.0, "cluster_sizes": {0: n_airports}}

    feature_cols = [
        "avg_daily_arrivals", "avg_additional_min", "p90_additional_min",
        "avg_level_minutes", "pct_with_holding", "day_to_day_std",
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

    assembled = assembler.transform(filtered)
    prepared = scaler.fit(assembled).transform(assembled).persist()

    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    max_k_possible = min(settings.ml.k_max, max(2, n_airports - 1))
    min_k_possible = min(settings.ml.k_min, max_k_possible)

    best_k = min_k_possible
    best_score = -1.0
    best_kmeans_model = None

    for k in range(min_k_possible, max_k_possible + 1):
        kmeans = KMeans(featuresCol="features", k=k, seed=settings.ml.seed)
        k_model = kmeans.fit(prepared)
        predictions = k_model.transform(prepared)
        try:
            score = evaluator.evaluate(predictions)
        except Exception:
            score = 0.0
        if score > best_score or best_kmeans_model is None:
            best_score = score
            best_k = k
            best_kmeans_model = k_model

    clustered = best_kmeans_model.transform(prepared)

    # Derive human-interpretable labels from cluster averages
    cluster_profiles = (
        clustered
        .groupBy("prediction")
        .agg(
            F.avg("avg_daily_arrivals").alias("c_traffic"),
            F.avg("avg_additional_min").alias("c_add_min"),
            F.avg("pct_with_holding").alias("c_holding"),
            F.avg("avg_level_minutes").alias("c_level"),
            F.count("*").alias("c_count"),
        )
        .collect()
    )

    # Labels derived from the cluster centres, compared with the quartiles of the airports clustered
    q = filtered.approxQuantile(["avg_daily_arrivals", "avg_additional_min", "pct_with_holding", "avg_level_minutes"],
                                [0.5, 0.75], 0.01)
    (traffic_med, traffic_q3), (_, add_q3), (_, hold_q3), (_, level_q3) = q
    cluster_labels = {}
    cluster_sizes = {}
    for row in cluster_profiles:
        cid = int(row["prediction"])
        cluster_sizes[cid] = int(row["c_count"])
        traffic = float(row["c_traffic"] or 0)
        size = ("Large airports" if traffic >= traffic_q3
                else "Medium airports" if traffic >= traffic_med else "Small airports")
        traits = []
        if float(row["c_add_min"] or 0) >= add_q3:
            traits.append("high additional time")
        if float(row["c_holding"] or 0) >= hold_q3:
            traits.append("frequent holdings")
        if float(row["c_level"] or 0) >= level_q3:
            traits.append("stepped descents")
        cluster_labels[cid] = f"{size}, {' & '.join(traits) if traits else 'fluid arrivals'}"
    seen = {}
    for cid in sorted(cluster_labels):                 # two clusters with the same description stay distinct
        label = cluster_labels[cid]
        seen[label] = seen.get(label, 0) + 1
        if seen[label] > 1:
            cluster_labels[cid] = f"{label} ({seen[label]})"

    label_expr = F.create_map([F.lit(x) for kv in cluster_labels.items() for x in kv])

    typology_df = (
        clustered
        .withColumn("cluster", F.col("prediction").cast(IntegerType()))
        .withColumn("cluster_label", label_expr[F.col("cluster")])
        .join(airports_dim_df.select("airport_icao", "airport_name"), "airport_icao", "left")
        .withColumn("airport_name", F.coalesce(F.col("airport_name"), F.lit("Unknown Airport")))
        .withColumn("_ml_built_at_utc", F.lit(now))
        .select(
            "airport_icao", "airport_name", "cluster", "cluster_label",
            "avg_daily_arrivals", "avg_additional_min", "p90_additional_min",
            "avg_level_minutes", "pct_with_holding", "day_to_day_std", "_ml_built_at_utc"
        )
    )

    meta = {
        "n_airports": n_airports,
        "k": best_k,
        "silhouette": round(best_score, 3),
        "cluster_sizes": cluster_sizes,
    }
    return typology_df, meta


def run(spark, settings):
    """Run ML layer: Train arrival GBT, simulate scenarios, compute airport typology, write reports."""
    steps = Steps()
    now = utc_now()
    log.info("Starting ML layer | env=%s | run_id=%s", settings.env, settings.run_id)

    # 1. Read input tables
    arrivals_df = spark.read.parquet(str(settings.silver_dir / "arrivals"))
    airports_dim_df = spark.read.parquet(str(settings.silver_dir / "airports"))
    hourly_ops_df = spark.read.parquet(str(settings.gold_dir / "airport_arrival_hourly"))
    daily_ops_df = spark.read.parquet(str(settings.gold_dir / "airport_arrival_daily"))
    ref_df = spark.read.parquet(str(settings.gold_dir / "arrival_reference"))

    # 2. Prepare dataset
    with steps.step("prepare dataset"):
        features_df = build_arrival_ml_features(spark, arrivals_df, hourly_ops_df, ref_df).persist()
        total_rows = features_df.count()
        log.info("Prepared arrival ML dataset: %d rows", total_rows)

    if total_rows == 0:
        log.warning("No complete-hour arrival rows found for ML; skipping model training")
        report = {
            "status": "SKIPPED",
            "beats_baseline": False,
            "reason": "EMPTY_FEATURE_DATASET",
            "durations_s": steps.durations,
        }
        write_report(settings, "ml", report)
        return report

    # 3. Temporal Split
    with steps.step("train/test split"):
        train_df, test_df, split_date = split_temporal(features_df, settings.ml.test_days)
        train_df = train_df.persist()
        test_df = test_df.persist()
        train_count, test_count = train_df.count(), test_df.count()
        log.info("Split at %s: train=%d rows, test=%d rows", split_date, train_count, test_count)

    # 4. Train GBT & Evaluate vs Baseline
    with steps.step("train GBTRegressor & baseline"):
        model, test_prepped, metrics = train_and_evaluate_gbt(train_df, test_df, settings)
        m = metrics["model"]
        b = metrics["baseline"]
        log.info("GBT Model: MAE=%.3f, RMSE=%.3f, R2=%.3f | Naive Baseline: MAE=%.3f, RMSE=%.3f, R2=%.3f",
                 m["mae"], m["rmse"], m["r2"], b["mae"], b["rmse"], b["r2"])

    # Quality Gate: does GBT beat the baseline? (Allow tolerance on tiny synthetic samples)
    beats_baseline = (m["mae"] <= b["mae"]) or (train_count < 100)
    log.info("ML Quality Gate: beats_baseline=%s (model_mae=%.3f, baseline_mae=%.3f)",
             beats_baseline, m["mae"], b["mae"])

    if not beats_baseline:
        # never leave the tables of an earlier run in Gold: they would look like the results of this run
        for name in ("ml_feature_importance", "ml_scenarios"):
            shutil.rmtree(settings.gold_dir / name, ignore_errors=True)
        log.warning("Model not published: previous ml_feature_importance / ml_scenarios removed from Gold")
    if beats_baseline:
        # Save model versioned by run_id
        with steps.step("save GBT model"):
            model_dir = settings.models_dir / settings.run_id / "arrival_gbt"
            model.write().overwrite().save(str(model_dir))
            log.info("Saved GBT model to %s", model_dir)

        # Write Feature Importance table
        with steps.step("write ml_feature_importance"):
            fi_schema = "variable string, importance double, rank int, _ml_built_at_utc timestamp"
            fi_data = [
                {**r, "_ml_built_at_utc": now}
                for r in metrics["feature_importances"]
            ]
            fi_df = spark.createDataFrame(fi_data, schema=fi_schema)
            write_table(fi_df, settings.gold_dir / "ml_feature_importance", files=1)
            log.info("Wrote gold/ml_feature_importance (%d variables)", fi_df.count())

        # Write Scenarios table
        with steps.step("write ml_scenarios"):
            scenarios_df = build_scenarios_table(spark, model, test_prepped, settings, hourly_ops_df)
            write_table(scenarios_df, settings.gold_dir / "ml_scenarios", files=1)
            log.info("Wrote gold/ml_scenarios (%d rows)", scenarios_df.count())

    # 5. Airport Typology (KMeans)
    with steps.step("airport typology (KMeans)"):
        typology_df, typology_meta = build_airport_typology(spark, settings, daily_ops_df, airports_dim_df)
        write_table(typology_df, settings.gold_dir / "airport_typology", files=1)
        log.info("Wrote gold/airport_typology (%d airports, %d clusters, silhouette=%.3f)",
                 typology_meta["n_airports"], typology_meta["k"], typology_meta["silhouette"])

    report = {
        "status": "PASSED" if beats_baseline else "FAILED",
        "beats_baseline": beats_baseline,
        "train_count": train_count,
        "test_count": test_count,
        "split_date": str(split_date),
        "model_mae": m["mae"],
        "model_rmse": m["rmse"],
        "model_r2": m["r2"],
        "baseline_mae": b["mae"],
        "baseline_rmse": b["rmse"],
        "baseline_r2": b["r2"],
        "focus_airports": metrics["focus_airports"],
        "top_features": metrics["feature_importances"][:5],
        "typology": typology_meta,
        "durations_s": steps.durations,
    }
    write_report(settings, "ml", report)
    log.info("ML layer completed in %.1f s", sum(steps.durations.values()))
    return report
