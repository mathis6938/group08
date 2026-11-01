"""Ingestion (landing zone): download the OPDI and OurAirports files of the scope into raw/, unchanged,
and record every file in raw/manifest.csv (URL, size, Last-Modified, SHA-256, run id, result).

A file already in raw/ is downloaded again only if the source published a new version of it (different size or
Last-Modified than the copy we have): EUROCONTROL republishes corrected files, and OurAirports is updated daily.
"""
import csv
import datetime
import hashlib
import os
import shutil
import time
import urllib.error
import urllib.request
from collections import Counter

from .common import Steps, free_gb, log, utc_now, write_report
from .config import PARQUET_TABLES

OK_RESULTS = ("downloaded", "already_present", "republished")


def months(start, end):
    """First day of each month overlapping [start, end[."""
    m = datetime.date(start.year, start.month, 1)
    while m < end:
        yield m
        m = datetime.date(m.year + (m.month == 12), m.month % 12 + 1, 1)


def windows(start, end, origin, days=10):
    """OPDI 10-day windows (counted from `origin`) overlapping [start, end[, as (first day, end) pairs."""
    w = origin + datetime.timedelta(days=((start - origin).days // days) * days)
    while w < end:
        yield w, w + datetime.timedelta(days=days)
        w += datetime.timedelta(days=days)


def source_files(settings):
    """Files of the scope, as (table, file name, URL)."""
    files = []
    for m in months(settings.start, settings.end):
        n = f"flight_list_{m:%Y%m}.parquet"
        files.append(("flight_list", n, f"{settings.base_url}/flight_list/{n}"))
    for table in ("flight_events", "measurements"):
        for w, w_end in windows(settings.start, settings.end, settings.origin):
            n = f"{table}_{w:%Y%m%d}_{w_end:%Y%m%d}.parquet"
            files.append((table, n, f"{settings.base_url}/{table}/{n}"))
    for n, url in settings.ref_urls.items():
        files.append(("reference", n, url))
    return files


def local_files(settings):
    """Files of the scope already in raw/, by table: {table: [paths]}."""
    found = {}
    for table, name, _ in source_files(settings):
        path = settings.raw_dir / table / name
        if path.exists():
            found.setdefault(table, []).append(str(path))
    return found


def head(url, user_agent="UiS-DAT535-group08"):
    """HTTP status, size in bytes and Last-Modified of a remote file (without downloading it)."""
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, int(r.headers.get("Content-Length") or 0), r.headers.get("Last-Modified", "")
    except urllib.error.HTTPError as e:
        return e.code, 0, ""
    except Exception as e:
        return type(e).__name__, 0, ""


def download(url, dest, retries=3, user_agent="UiS-DAT535-group08", wait_s=5):
    """Download to a temporary .part file, then rename: an interrupted download never leaves a partial file."""
    tmp = dest.with_name(dest.name + ".part")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=300) as r, open(tmp, "wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)                  # 1 MB chunks
            os.replace(tmp, dest)
            return "downloaded"
        except Exception as e:
            log.warning("    attempt %d/%d failed: %s", attempt, retries, e)
            tmp.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(wait_s * attempt)
    return "failed"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def last_manifest_entries(settings):
    """Latest successful manifest entry of each file (the manifest is chronological: the last entry wins)."""
    latest = {}
    if settings.manifest_path.exists():
        with open(settings.manifest_path, newline="") as f:
            for r in csv.DictReader(f):
                if r["result"] in OK_RESULTS:
                    latest[r["file"]] = r
    return latest


def is_republished(local_path, remote_size, remote_last_modified, previous):
    """True if the source holds another version of a file we already have."""
    if remote_size and local_path.stat().st_size != remote_size:
        return True
    old = (previous or {}).get("last_modified", "")
    return bool(old and remote_last_modified and old != remote_last_modified)


def run(settings):
    """Download the files of the scope that are not in raw/ yet, and append one manifest row per file."""
    steps = Steps()
    files = source_files(settings)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    log.info("Period [%s, %s[ | %d files | destination %s | free %.1f GB",
             settings.start, settings.end, len(files), settings.raw_dir, free_gb(settings.raw_dir))
    rows = []
    previous = last_manifest_entries(settings)
    with steps.step("download"):
        for i, (table, name, url) in enumerate(files, 1):
            status, size, last_modified = head(url, settings.user_agent)
            dest = settings.raw_dir / table / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            if status != 200:
                result = "absent"
            elif size == 0:
                result = "empty"
            elif dest.exists() and not is_republished(dest, size, last_modified, previous.get(name)):
                result = "already_present"
            elif free_gb(settings.raw_dir) - size / 1e9 < settings.min_free_gb:
                log.error("STOP: insufficient free space before %s", name)
                break
            else:
                republished = dest.exists()          # new version: the .part download replaces the old copy
                result = download(url, dest, settings.retries, settings.user_agent)
                if republished and result == "downloaded":
                    result = "republished"
                    log.warning("    %s was republished by the source: new version downloaded", name)
            ok = result in OK_RESULTS
            rows.append(dict(run_id=settings.run_id, table=table, file=name, url=url, http_status=status,
                             size_bytes=size, last_modified=last_modified, result=result,
                             sha256=sha256(dest) if ok else "", local_path=str(dest) if ok else "",
                             retrieved_at_utc=utc_now().isoformat(timespec="seconds")))
            log.info("[%2d/%d] %-13s %-45s %8.1f MB  %s", i, len(files), table, name, size / 1e6, result)
            time.sleep(settings.pause_s)

    if rows:                                                       # the manifest keeps the history of all runs
        new = not settings.manifest_path.exists()
        with open(settings.manifest_path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            if new:
                w.writeheader()
            w.writerows(rows)

    results = Counter(r["result"] for r in rows)
    sizes = {t: round(sum(int(r["size_bytes"]) for r in rows if r["table"] == t and r["local_path"]) / 1e9, 3)
             for t in (*PARQUET_TABLES, "reference")}
    problems = [dict(file=r["file"], result=r["result"], http_status=r["http_status"])
                for r in rows if r["result"] not in OK_RESULTS]
    log.info("Results: %s | sizes (GB): %s | free space left %.1f GB",
             dict(results), sizes, free_gb(settings.raw_dir))
    for p in problems:
        log.warning("  ! %s -> %s (HTTP %s)", p["file"], p["result"], p["http_status"])
    return write_report(settings, "ingest", dict(
        files_expected=len(files), results=dict(results), size_gb=sizes, problems=problems,
        manifest=str(settings.manifest_path), durations_s=steps.durations))
