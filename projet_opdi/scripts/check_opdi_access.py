#!/usr/bin/env python3
"""
Vérification d'accès aux données OPDI (EUROCONTROL / OpenSky) — à lancer sur la VM Spark.

Étapes :
  1. Disponibilité (HEAD) de tous les fichiers mensuels flight_list 2022-01 -> aujourd'hui
  2. Téléchargement d'un mois de flight_list + d'un fichier flight_events (10 jours)
     + référentiel OurAirports (airports.csv)
  3. Lecture avec PySpark : schéma, nombre de lignes, aperçu, quelques comptes qualité
     + vols touchant LFBO (Toulouse) et ENZV (Stavanger)

Les données sont écrites HORS du dépôt Git (par défaut ~/opdi-data/exploration).

Usage (sur la VM) :
  source ~/spark-env/bin/activate
  python scripts/check_opdi_access.py --events-start 20251231
  python scripts/check_opdi_access.py --month 202503 --skip-head
"""
import argparse
import datetime as dt
import os
import sys
import time
import urllib.error
import urllib.request

BASE = "https://www.eurocontrol.int/performance/data/download/OPDI/v002"
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
DEFAULT_OUT = "~/opdi-data/exploration"
UA = {"User-Agent": "UiS-DAT535-student-project/0.1"}


def head(url):
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, int(r.headers.get("Content-Length") or 0)
    except urllib.error.HTTPError as e:
        return e.code, 0
    except Exception as e:  # réseau, DNS, timeout
        return repr(e), 0


def download(url, dest):
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print(f"  déjà présent : {dest}")
        return dest
    t0 = time.time()
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r, open(dest + ".part", "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    os.replace(dest + ".part", dest)
    size = os.path.getsize(dest) / 1e6
    print(f"  OK {size:.1f} Mo en {time.time() - t0:.1f}s -> {dest}")
    return dest


def months(start=(2022, 1)):
    y, m = start
    today = dt.date.today()
    while (y, m) <= (today.year, today.month):
        yield f"{y}{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default="202601", help="YYYYMM du flight_list à télécharger")
    ap.add_argument("--events-start", default="20251231", help="YYYYMMDD début du fichier events")
    ap.add_argument("--skip-head", action="store_true")
    ap.add_argument("--out", default=DEFAULT_OUT, help="dossier de sortie des données (hors dépôt)")
    a = ap.parse_args()
    global OUT
    OUT = os.path.expanduser(a.out)
    os.makedirs(OUT, exist_ok=True)

    # 1. Disponibilité
    if not a.skip_head:
        print("\n=== 1. Disponibilité des fichiers flight_list mensuels ===")
        total, missing = 0, []
        for ym in months():
            st, size = head(f"{BASE}/flight_list/flight_list_{ym}.parquet")
            ok = st == 200
            total += size if ok else 0
            if not ok:
                missing.append((ym, st))
            print(f"  {ym}: {st} {size/1e6:8.1f} Mo")
        print(f"  Total disponible ≈ {total/1e9:.2f} Go ; manquants : {missing or 'aucun'}")

    # 2. Téléchargement
    print("\n=== 2. Téléchargements ===")
    fl = download(f"{BASE}/flight_list/flight_list_{a.month}.parquet",
                  f"{OUT}/flight_list_{a.month}.parquet")
    s = dt.datetime.strptime(a.events_start, "%Y%m%d")
    e = (s + dt.timedelta(days=10)).strftime("%Y%m%d")
    ev_name = f"flight_events_{a.events_start}_{e}.parquet"
    try:
        ev = download(f"{BASE}/flight_events/{ev_name}", f"{OUT}/{ev_name}")
    except Exception as ex:
        print(f"  ÉCHEC events ({ex}) — vérifier le nommage exact sur opdi.aero")
        ev = None
    apt = download(AIRPORTS_URL, f"{OUT}/airports.csv")

    # 3. Inspection avec PySpark
    print("\n=== 3. Inspection PySpark ===")
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.appName("opdi-check")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(fl)
    print(f"\n--- flight_list {a.month} ---")
    df.printSchema()
    n = df.count()
    print(f"Lignes : {n:,}")
    df.show(5, truncate=False)

    print("Valeurs nulles par colonne (%):")
    nulls = df.select([(F.sum(F.col(c).isNull().cast("int")) / n * 100).alias(c)
                       for c in df.columns]).collect()[0].asDict()
    for c, v in nulls.items():
        print(f"  {c:30s} {v:6.2f}")

    print(f"Doublons exacts : {n - df.dropDuplicates().count():,}")

    # Colonnes aéroport : détection souple (noms exacts inconnus à l'avance)
    apt_cols = [c for c in df.columns if any(k in c.lower() for k in ("adep", "ades", "airport", "origin", "dest"))]
    print(f"Colonnes aéroport détectées : {apt_cols}")
    for code in ("LFBO", "ENZV"):
        if apt_cols:
            cond = None
            for c in apt_cols:
                cc = F.col(c) == code
                cond = cc if cond is None else (cond | cc)
            print(f"  Vols touchant {code} : {df.filter(cond).count():,}")

    if ev:
        dfe = spark.read.parquet(ev)
        print(f"\n--- {ev_name} ---")
        dfe.printSchema()
        print(f"Lignes : {dfe.count():,}")
        dfe.show(5, truncate=False)
        typ = [c for c in dfe.columns if "type" in c.lower()]
        if typ:
            dfe.groupBy(typ[0]).count().orderBy(F.desc("count")).show(30, truncate=False)

    dfa = spark.read.option("header", True).csv(apt)
    print(f"\n--- airports.csv --- {dfa.count():,} lignes")
    dfa.filter(F.col("ident").isin("LFBO", "ENZV")) \
       .select("ident", "name", "type", "latitude_deg", "longitude_deg", "iso_country").show(truncate=False)

    spark.stop()
    print("\nVérification terminée.")


if __name__ == "__main__":
    sys.exit(main())
