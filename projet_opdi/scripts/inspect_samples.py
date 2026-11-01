#!/usr/bin/env python3
"""
Inspection d'UN fichier par type OPDI (même période : début juin 2025).

Pré-requis : fichiers téléchargés dans ~/opdi-data/samples/ (voir README / commandes curl).
Usage (sur la VM, depuis group08/projet_opdi) :
  python scripts/inspect_samples.py > ~/opdi-data/samples/inspection.txt 2>&1
"""
import os

import findspark
findspark.init()  # comme dans les labs : Spark 3.5 de /opt/spark (Java 8)

from pyspark.sql import SparkSession, functions as F

D = os.path.expanduser("~/opdi-data/samples")
FILES = {
    "FLIGHT": "flight_list_202506.parquet",
    "EVENT": "flight_events_20250604_20250614.parquet",
    "MEASUREMENT": "measurements_20250604_20250614.parquet",
    "AIRPORT": "airports.csv",
    "RUNWAY": "runways.csv",
}

spark = (SparkSession.builder.appName("opdi-inspect-samples")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.memory", "2g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def load(name):
    path = os.path.join(D, FILES[name])
    if not os.path.exists(path):
        print(f"\n### {name} : fichier absent ({path})")
        return None
    if path.endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    return spark.read.parquet(path)


def profile(name, df, group_col=None):
    print("\n" + "=" * 100)
    print(f"### {name} — {FILES[name]} ({os.path.getsize(os.path.join(D, FILES[name]))/1e6:.1f} Mo)")
    print("=" * 100)
    df.printSchema()
    n = df.count()
    print(f"Lignes : {n:,}")
    df.show(5, truncate=80)
    print("Valeurs nulles (%) :")
    row = df.select([(F.sum(F.col(c).isNull().cast("int")) / n * 100).alias(c) for c in df.columns]).first()
    for c, v in row.asDict().items():
        print(f"  {c:30s} {v:6.2f}")
    if group_col and group_col in df.columns:
        print(f"Répartition par {group_col} :")
        df.groupBy(group_col).count().orderBy(F.desc("count")).show(60, truncate=False)
    return n


dfs = {k: load(k) for k in FILES}

if dfs["FLIGHT"] is not None:
    profile("FLIGHT", dfs["FLIGHT"])
if dfs["EVENT"] is not None:
    profile("EVENT", dfs["EVENT"], "type")
if dfs["MEASUREMENT"] is not None:
    m = dfs["MEASUREMENT"]
    gc = next((c for c in m.columns if c.lower() in ("type", "measurement_type", "name")), None)
    profile("MEASUREMENT", m, gc)
    if "info" in m.columns:
        m.select("info").where(F.col("info").isNotNull()).show(5, truncate=False)
if dfs["AIRPORT"] is not None:
    profile("AIRPORT", dfs["AIRPORT"], "type")
if dfs["RUNWAY"] is not None:
    r = dfs["RUNWAY"]
    profile("RUNWAY", r)
    r.filter(F.col("airport_ident").isin("LFBO", "ENZV")).show(truncate=False)

# Liens entre tables (même période)
print("\n" + "=" * 100)
print("### LIENS ENTRE TABLES")
print("=" * 100)
f, e, m = dfs["FLIGHT"], dfs["EVENT"], dfs["MEASUREMENT"]
if f is not None and e is not None:
    ev_ids = e.select(F.col("flight_id").alias("fid")).distinct()
    n_ev = ev_ids.count()
    matched = ev_ids.join(f.select(F.col("id").alias("fid")), "fid", "inner").count()
    print(f"EVENT : {n_ev:,} vols distincts, dont {matched:,} trouvés dans FLIGHT 202506 "
          f"({matched / max(n_ev, 1) * 100:.1f} %)")
    # Couverture : part des vols de la période (4-14 juin) ayant au moins un événement
    fp = f.filter((F.col("first_seen") >= "2025-06-04") & (F.col("first_seen") < "2025-06-14"))
    n_fp = fp.count()
    cov = fp.select(F.col("id").alias("fid")).join(ev_ids, "fid", "inner").count()
    print(f"FLIGHT 4-14 juin : {n_fp:,} vols, dont {cov:,} avec au moins un événement "
          f"({cov / max(n_fp, 1) * 100:.1f} %)")
if m is not None:
    print(f"Colonnes MEASUREMENT : {m.columns}")
    link = next((c for c in m.columns if c in ("event_id", "flight_id")), None)
    if link == "event_id" and e is not None:
        n_m = m.select("event_id").distinct().count()
        ok = m.select("event_id").distinct().join(e.select(F.col("id").alias("event_id")), "event_id").count()
        print(f"MEASUREMENT -> EVENT : {ok:,}/{n_m:,} event_id retrouvés ({ok / max(n_m, 1) * 100:.1f} %)")
    elif link == "flight_id" and f is not None:
        n_m = m.select("flight_id").distinct().count()
        ok = m.select("flight_id").distinct().join(f.select(F.col("id").alias("flight_id")), "flight_id").count()
        print(f"MEASUREMENT -> FLIGHT : {ok:,}/{n_m:,} flight_id retrouvés ({ok / max(n_m, 1) * 100:.1f} %)")

spark.stop()
print("\nInspection terminée.")
