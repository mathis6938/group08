#!/usr/bin/env python3
"""
Téléchargement de TOUS les fichiers OPDI v0.0.2 + référentiels OurAirports sur la VM.

Tables :
  - flight_list   : 1 fichier Parquet par mois           (2022-01 -> dernier mois publié)
  - flight_events : 1 fichier Parquet par tranche de 10 j (en continu depuis 2022-01-01)
  - measurements  : 1 fichier Parquet par tranche de 10 j (même découpage)
  - airports.csv, runways.csv (OurAirports)

Sécurités :
  - --dry-run : ne télécharge rien, interroge seulement la taille de chaque fichier (HEAD)
                et affiche le volume total par table -> à lancer EN PREMIER
  - arrêt automatique si l'espace libre passe sous --min-free-gb (défaut 4 Go)
  - reprise : un fichier déjà présent avec la bonne taille n'est pas retéléchargé
  - écriture en .part puis renommage (pas de fichier à moitié écrit)
  - 3 tentatives par fichier, pause entre les requêtes (respect du serveur)
  - manifeste CSV (url, taille, Last-Modified, statut, date) pour la traçabilité

Usage (sur la VM, depuis group08/projet_opdi) :
  python scripts/download_all_opdi.py --dry-run
  nohup python scripts/download_all_opdi.py > ~/opdi-data/download_all.log 2>&1 &
  tail -f ~/opdi-data/download_all.log
Options :
  --tables flight_list flight_events     (sous-ensemble de tables)
  --out ~/opdi-data/raw                  (dossier de destination, hors dépôt Git)
"""
import argparse
import csv
import datetime as dt
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

BASE = "https://www.eurocontrol.int/performance/data/download/OPDI/v002"
REF = {
    "airports.csv": "https://davidmegginson.github.io/ourairports-data/airports.csv",
    "runways.csv": "https://davidmegginson.github.io/ourairports-data/runways.csv",
}
UA = {"User-Agent": "UiS-DAT535-student-project/0.1"}
START = dt.date(2022, 1, 1)
ALL_TABLES = ["flight_list", "flight_events", "measurements", "reference"]


def head(url):
    """Retourne (status, taille_octets, last_modified)."""
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, int(r.headers.get("Content-Length") or 0), r.headers.get("Last-Modified", "")
    except urllib.error.HTTPError as e:
        return e.code, 0, ""
    except Exception as e:
        return repr(e), 0, ""


def free_gb(path):
    return shutil.disk_usage(path).free / 1e9


def months():
    y, m = START.year, START.month
    today = dt.date.today()
    while (y, m) <= (today.year, today.month):
        yield f"{y}{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def ten_day_windows():
    d = START
    today = dt.date.today()
    while d <= today:
        yield d.strftime("%Y%m%d"), (d + dt.timedelta(days=10)).strftime("%Y%m%d")
        d += dt.timedelta(days=10)


def build_file_list(tables):
    """Liste (table, nom_fichier, url)."""
    files = []
    if "flight_list" in tables:
        for ym in months():
            n = f"flight_list_{ym}.parquet"
            files.append(("flight_list", n, f"{BASE}/flight_list/{n}"))
    if "flight_events" in tables:
        for s, e in ten_day_windows():
            n = f"flight_events_{s}_{e}.parquet"
            files.append(("flight_events", n, f"{BASE}/flight_events/{n}"))
    if "measurements" in tables:
        for s, e in ten_day_windows():
            n = f"measurements_{s}_{e}.parquet"
            files.append(("measurements", n, f"{BASE}/measurements/{n}"))
    if "reference" in tables:
        for n, url in REF.items():
            files.append(("reference", n, url))
    return files


def download(url, dest, expected_size):
    tmp = dest + ".part"
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=300) as r, open(tmp, "wb") as f:
                while chunk := r.read(1 << 20):
                    f.write(chunk)
            got = os.path.getsize(tmp)
            if expected_size and got != expected_size:
                raise IOError(f"taille reçue {got} != attendue {expected_size}")
            os.replace(tmp, dest)
            return "ok"
        except Exception as ex:
            print(f"    tentative {attempt}/3 échouée : {ex}", flush=True)
            if os.path.exists(tmp):
                os.remove(tmp)
            time.sleep(5 * attempt)
    return "failed"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="~/opdi-data/raw")
    ap.add_argument("--tables", nargs="+", default=ALL_TABLES, choices=ALL_TABLES)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-free-gb", type=float, default=4.0)
    ap.add_argument("--pause", type=float, default=0.5, help="pause (s) entre deux requêtes")
    a = ap.parse_args()

    out = os.path.expanduser(a.out)
    os.makedirs(out, exist_ok=True)
    files = build_file_list(a.tables)
    print(f"{len(files)} fichiers candidats ; espace libre : {free_gb(out):.1f} Go", flush=True)

    manifest_path = os.path.join(out, "manifest.csv")
    new_manifest = not os.path.exists(manifest_path)
    mf = None if a.dry_run else open(manifest_path, "a", newline="")
    mw = csv.writer(mf) if mf else None
    if mw and new_manifest:
        mw.writerow(["table", "file", "url", "http_status", "size_bytes", "last_modified", "result", "utc_time"])

    totals, counts, missing = {}, {}, []
    t0 = time.time()
    for i, (table, name, url) in enumerate(files, 1):
        st, size, lm = head(url)
        time.sleep(a.pause)
        if st != 200 or size == 0:
            missing.append((name, st, size))
            result = "absent" if st != 200 else "empty"
            print(f"[{i}/{len(files)}] {name}: {result} ({st})", flush=True)
            if mw:
                mw.writerow([table, name, url, st, size, lm, result, dt.datetime.utcnow().isoformat()])
            continue
        totals[table] = totals.get(table, 0) + size
        counts[table] = counts.get(table, 0) + 1
        if a.dry_run:
            print(f"[{i}/{len(files)}] {name}: {size/1e6:8.1f} Mo", flush=True)
            continue

        d = os.path.join(out, table)
        os.makedirs(d, exist_ok=True)
        dest = os.path.join(d, name)
        if os.path.exists(dest) and os.path.getsize(dest) == size:
            result = "already_present"
        else:
            if free_gb(out) - size / 1e9 < a.min_free_gb:
                print(f"\nARRÊT : espace libre {free_gb(out):.1f} Go, seuil {a.min_free_gb} Go atteint "
                      f"avant {name}. Relancer après avoir libéré de la place (reprise automatique).", flush=True)
                break
            result = download(url, dest, size)
            time.sleep(a.pause)
        print(f"[{i}/{len(files)}] {name}: {size/1e6:8.1f} Mo -> {result} "
              f"(libre : {free_gb(out):.1f} Go, {time.time()-t0:.0f}s)", flush=True)
        mw.writerow([table, name, url, st, size, lm, result, dt.datetime.utcnow().isoformat()])
        mf.flush()

    print("\n=== Résumé ===")
    for t in totals:
        print(f"  {t:15s} {counts[t]:4d} fichiers  {totals[t]/1e9:7.2f} Go")
    print(f"  {'TOTAL':15s} {sum(counts.values()):4d} fichiers  {sum(totals.values())/1e9:7.2f} Go")
    print(f"  Absents / vides : {len(missing)} -> {[m[0] for m in missing]}")
    print(f"  Espace libre restant : {free_gb(out):.1f} Go")
    if mf:
        mf.close()
        print(f"  Manifeste : {manifest_path}")


if __name__ == "__main__":
    sys.exit(main())
