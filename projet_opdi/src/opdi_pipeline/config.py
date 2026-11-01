"""Settings of one environment (dev / prod), read from projet_opdi/config/<env>.toml.

Precedence: explicit arguments (command line) > environment variables (GitHub Actions, tests) > TOML file.
"""
import datetime
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]          # projet_opdi/
CONFIG_DIR = PROJECT_DIR / "config"
PARQUET_TABLES = ("flight_list", "flight_events", "measurements")

# environment variable -> setting
ENV_VARS = {
    "OPDI_START": "start", "OPDI_END": "end", "OPDI_DATA_ROOT": "data_root", "OPDI_RUN_ID": "run_id",
    "OPDI_BASE_URL": "base_url", "OPDI_AIRPORTS_URL": "airports_url", "OPDI_RUNWAYS_URL": "runways_url",
    "OPDI_FULL_REFRESH": "full_refresh",
}


@dataclass(frozen=True)
class Settings:
    env: str
    start: datetime.date            # inclusive
    end: datetime.date              # exclusive
    data_root: Path
    base_url: str
    airports_url: str
    runways_url: str
    origin: datetime.date
    min_free_gb: float
    retries: int
    pause_s: float
    user_agent: str
    app_name: str
    driver_memory: str
    shuffle_partitions: int
    run_id: str                     # shared by all the layers of one run (reports/<run_id>/)
    full_refresh: bool = False      # rebuild every Bronze period, even if its source file did not change

    @property
    def raw_dir(self):
        return self.data_root / "raw"

    @property
    def bronze_dir(self):
        return self.data_root / "bronze"

    @property
    def silver_dir(self):
        return self.data_root / "silver"

    @property
    def quarantine_dir(self):
        return self.data_root / "quarantine"

    @property
    def gold_dir(self):
        return self.data_root / "gold"

    @property
    def report_dir(self):
        return self.data_root / "reports" / self.run_id

    @property
    def manifest_path(self):
        return self.raw_dir / "manifest.csv"

    @property
    def schema_report_path(self):
        return self.raw_dir / "schema_report.csv"

    @property
    def ref_urls(self):
        return {"airports.csv": self.airports_url, "runways.csv": self.runways_url}


def _bool(value):
    return value if isinstance(value, bool) else str(value).strip().lower() in ("1", "true", "yes")


def _date(value):
    return value if isinstance(value, datetime.date) else datetime.date.fromisoformat(str(value))


def load_settings(env="prod", **overrides):
    """Settings of `env` ("dev" or "prod"); keyword arguments (e.g. start="2026-06-01") override everything."""
    path = CONFIG_DIR / f"{env}.toml"
    if not path.exists():
        raise ValueError(f"unknown environment {env!r}: no {path}")
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    values = {**cfg["scope"], **cfg["paths"], **cfg["sources"], **cfg["ingestion"], **cfg["spark"]}
    for var, key in ENV_VARS.items():
        if os.environ.get(var):
            values[key] = os.environ[var]
    values.update({k: v for k, v in overrides.items() if v not in (None, "")})
    values.setdefault("run_id", datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

    settings = Settings(
        env=env, start=_date(values["start"]), end=_date(values["end"]),
        data_root=Path(str(values["data_root"])).expanduser(),
        base_url=values["base_url"], airports_url=values["airports_url"], runways_url=values["runways_url"],
        origin=_date(values["origin"]), min_free_gb=float(values["min_free_gb"]), retries=int(values["retries"]),
        pause_s=float(values["pause_s"]), user_agent=values["user_agent"], app_name=values["app_name"],
        driver_memory=str(values["driver_memory"]), shuffle_partitions=int(values["shuffle_partitions"]),
        run_id=str(values["run_id"]), full_refresh=_bool(values.get("full_refresh", False)))
    if settings.start >= settings.end:
        raise ValueError(f"empty scope: start {settings.start} must be before end {settings.end}")
    return settings
