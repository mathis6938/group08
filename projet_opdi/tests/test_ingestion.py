"""Ingestion (Part 2.1): file names of the OPDI periods, download, checksum."""
import datetime
import functools
import hashlib
import http.server
import threading

import pytest

D = datetime.date


def test_months_cover_the_scope(nb):
    assert list(nb["months"](D(2026, 6, 1), D(2026, 7, 29))) == [D(2026, 6, 1), D(2026, 7, 1)]
    assert list(nb["months"](D(2025, 12, 15), D(2026, 1, 2))) == [D(2025, 12, 1), D(2026, 1, 1)]


def test_windows_are_aligned_on_origin(nb):
    # Real EUROCONTROL file names: the 10-day windows are counted from 2022-01-01
    assert list(nb["windows"](D(2026, 1, 1), D(2026, 1, 2))) == [(D(2025, 12, 31), D(2026, 1, 10))]
    w = list(nb["windows"](D(2026, 6, 1), D(2026, 7, 29)))
    assert w[0] == (D(2026, 5, 30), D(2026, 6, 9))           # flight_events_20260530_20260609
    assert w[-1] == (D(2026, 7, 19), D(2026, 7, 29))         # flight_events_20260719_20260729
    assert len(w) == 6
    assert all(b - a == datetime.timedelta(days=10) for a, b in w)
    assert all(w[i][1] == w[i + 1][0] for i in range(len(w) - 1))   # no gap, no overlap


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


def test_head_and_download(nb, http_site, tmp_path):
    site, url = http_site
    (site / "f.parquet").write_bytes(b"x" * 1000)
    status, size, last_modified = nb["head"](f"{url}/f.parquet")
    assert (status, size) == (200, 1000) and last_modified
    assert nb["head"](f"{url}/missing.parquet")[0] == 404

    dest = tmp_path / "f.parquet"
    assert nb["download"](f"{url}/f.parquet", dest) == "downloaded"
    assert dest.read_bytes() == b"x" * 1000
    assert not dest.with_name("f.parquet.part").exists()      # temporary file removed
    assert nb["sha256"](dest) == hashlib.sha256(b"x" * 1000).hexdigest()


def test_failed_download_leaves_nothing(nb, http_site, tmp_path, monkeypatch):
    _, url = http_site
    monkeypatch.setitem(nb, "RETRIES", 1)
    dest = tmp_path / "missing.parquet"
    assert nb["download"](f"{url}/missing.parquet", dest) == "failed"
    assert not dest.exists() and not dest.with_name("missing.parquet.part").exists()
