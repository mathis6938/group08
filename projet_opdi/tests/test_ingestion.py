"""Ingestion: file names of the OPDI periods, download, checksum."""
import datetime
import functools
import hashlib
import http.server
import os
import threading
import time

import pytest

from opdi_pipeline import ingestion
from opdi_pipeline.config import load_settings

D = datetime.date
ORIGIN = D(2022, 1, 1)


def test_months_cover_the_scope():
    assert list(ingestion.months(D(2026, 6, 1), D(2026, 7, 29))) == [D(2026, 6, 1), D(2026, 7, 1)]
    assert list(ingestion.months(D(2025, 12, 15), D(2026, 1, 2))) == [D(2025, 12, 1), D(2026, 1, 1)]


def test_windows_are_aligned_on_origin():
    # Real EUROCONTROL file names: the 10-day windows are counted from 2022-01-01
    assert list(ingestion.windows(D(2026, 1, 1), D(2026, 1, 2), ORIGIN)) == [(D(2025, 12, 31), D(2026, 1, 10))]
    w = list(ingestion.windows(D(2026, 6, 1), D(2026, 7, 29), ORIGIN))
    assert w[0] == (D(2026, 5, 30), D(2026, 6, 9))           # flight_events_20260530_20260609
    assert w[-1] == (D(2026, 7, 19), D(2026, 7, 29))         # flight_events_20260719_20260729
    assert len(w) == 6
    assert all(b - a == datetime.timedelta(days=10) for a, b in w)
    assert all(w[i][1] == w[i + 1][0] for i in range(len(w) - 1))   # no gap, no overlap


def test_source_files_of_the_prod_scope():
    files = ingestion.source_files(load_settings("prod"))
    names = [n for _, n, _ in files]
    assert names[:2] == ["flight_list_202606.parquet", "flight_list_202607.parquet"]
    assert "flight_events_20260530_20260609.parquet" in names and "measurements_20260719_20260729.parquet" in names
    assert names[-2:] == ["airports.csv", "runways.csv"]
    assert len(files) == 2 + 6 + 6 + 2


@pytest.fixture
def http_site(tmp_path):
    site = tmp_path / "site"
    site.mkdir()
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(site))
    handler.log_message = lambda *a, **k: None
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield site, f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_head_and_download(http_site, tmp_path):
    site, url = http_site
    (site / "f.parquet").write_bytes(b"x" * 1000)
    status, size, last_modified = ingestion.head(f"{url}/f.parquet")
    assert (status, size) == (200, 1000) and last_modified
    assert ingestion.head(f"{url}/missing.parquet")[0] == 404

    dest = tmp_path / "f.parquet"
    assert ingestion.download(f"{url}/f.parquet", dest) == "downloaded"
    assert dest.read_bytes() == b"x" * 1000
    assert not dest.with_name("f.parquet.part").exists()      # temporary file removed
    assert ingestion.sha256(dest) == hashlib.sha256(b"x" * 1000).hexdigest()


def test_failed_download_leaves_nothing(http_site, tmp_path):
    _, url = http_site
    dest = tmp_path / "missing.parquet"
    assert ingestion.download(f"{url}/missing.parquet", dest, retries=2, wait_s=0) == "failed"
    assert not dest.exists() and not dest.with_name("missing.parquet.part").exists()


def test_republished_files_are_downloaded_again(http_site, tmp_path):
    """A file is downloaded again only when the source holds another version (size or Last-Modified changed)."""
    site, url = http_site
    for table, name in [("flight_list", "flight_list_202606.parquet"),
                        ("flight_events", "flight_events_20260609_20260619.parquet"),
                        ("measurements", "measurements_20260609_20260619.parquet")]:
        (site / "opdi" / table).mkdir(parents=True)
        (site / "opdi" / table / name).write_bytes(b"v1" * 100)
    (site / "airports.csv").write_text("ident\nLFBO\n")
    (site / "runways.csv").write_text("airport_ident\nLFBO\n")
    settings = load_settings("dev", data_root=str(tmp_path / "data"), base_url=f"{url}/opdi", pause_s=0,
                             airports_url=f"{url}/airports.csv", runways_url=f"{url}/runways.csv")

    assert ingestion.run(settings)["results"] == {"downloaded": 5}
    assert ingestion.run(settings)["results"] == {"already_present": 5}

    events = site / "opdi" / "flight_events" / "flight_events_20260609_20260619.parquet"
    events.write_bytes(b"v2" * 150)                              # corrected file, other size
    later = time.time() + 120
    runways = site / "runways.csv"
    os.utime(runways, (later, later))                            # same content, new Last-Modified
    report = ingestion.run(settings)
    assert report["results"] == {"republished": 2, "already_present": 3}
    assert (settings.raw_dir / "flight_events" / events.name).read_bytes() == b"v2" * 150
    latest = ingestion.last_manifest_entries(settings)
    assert latest[events.name]["sha256"] == hashlib.sha256(b"v2" * 150).hexdigest()   # lineage of the new version
