"""Weather of the destination airports (METAR), integrated into the Medallion layers.

The professors suggested adding the weather to explain the arrival delays. METAR are the weather reports that every
airport publishes every 30 or 60 minutes (visibility, clouds, wind, present weather). They come from the public
archive of the Iowa Environmental Mesonet (IEM), for the real period of the scope, for the airports listed in the
[weather] section of the configuration.

    ingestion   raw/weather/metar_<ICAO>_<start>_<end>.csv, recorded in the manifest           (ingestion.py)
    bronze      bronze/metar: CSV columns as text + lineage, schema contract, reconciliation  (bronze.py)
    silver      silver/weather_observations: one row per METAR, typed, quality rules, quarantine,
                deduplication, weather conditions of the observation                          (this module)
    gold        gold/airport_weather_hourly (one row per airport hour, weather category) and
                gold/arrival_weather_impact (arrival indicators by weather category)          (sql/, gold.py)
    ml          gold/ml_weather_comparison: the arrival GBT of ml.py trained on the same flights without and with
                the weather of the arrival hour, against the naive baseline                   (this module, ml.py)

Weather conditions of an observation (thresholds in config/*.toml, [weather]):
    IMC (instrument conditions)  visibility < 5 km or ceiling < 1 500 ft (ICAO Annex 2, VFR in a control zone)
    below CAT I                  visibility < 800 m or ceiling < 200 ft (ICAO CAT I approach minima)
    strong wind                  mean wind >= 20 kt or gusts >= 30 kt (our threshold)
    thunderstorm, precipitation, fog: from the present-weather codes of the METAR (not from the TEMPO forecast)
The ceiling is the height of the lowest broken (BKN), overcast (OVC) or vertical-visibility (VV) layer; no such
layer means no ceiling (NULL). Nothing is imputed: an hour without observation has no weather row.
"""
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

from .common import log, write_table

METAR_COLUMNS = ("station", "valid", "tmpc", "dwpc", "relh", "drct", "sknt", "gust", "vsby", "skyc1", "skyc2",
                 "skyc3", "skyc4", "skyl1", "skyl2", "skyl3", "skyl4", "wxcodes", "metar")
LINEAGE = ("_source_file", "_ingestion_run_id", "_source_sha256")
CEILING_COVERS = ("BKN", "OVC", "VV")
SILVER_COLUMNS = [
    "station_icao", "obs_time_utc", "obs_date", "obs_hour", "temp_c", "dewpoint_c", "rel_humidity_pct",
    "wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "visibility_m", "ceiling_ft", "present_weather",
    "has_thunderstorm", "has_precipitation", "has_fog", "is_imc", "is_below_cat1", "is_strong_wind", "raw_metar",
    "_silver_processed_at_utc", "_source_file", "_ingestion_run_id", "_source_sha256"]


# ---------------------------------------------------------------------------------------------- silver
def _num(c):
    """Numeric METAR field: 'M' (missing), empty or any non-number -> NULL."""
    v = F.trim(F.col(c))
    return F.when(v.rlike(r"^-?[0-9]+(\.[0-9]+)?$"), v.cast(DoubleType()))


def _code(c):
    """Text METAR field: upper case; 'M', '' and '///' (not observed) -> NULL."""
    v = F.upper(F.trim(F.col(c)))
    return F.when(~v.isin("M", "", "///"), v)


def standardize_metar(bronze_metar, weather, start, end):
    """Typed METAR observations + weather conditions of each observation + scope column."""
    wx = F.coalesce(F.col("present_weather"), F.lit(""))
    scope_start, scope_end = F.lit(str(start)).cast("timestamp"), F.lit(str(end)).cast("timestamp")
    return (bronze_metar
        .select(
            F.upper(F.trim(F.col("station"))).alias("station_icao"),
            F.to_timestamp(F.trim(F.col("valid")), "yyyy-MM-dd HH:mm").alias("obs_time_utc"),   # archive in UTC
            _num("tmpc").alias("temp_c"),
            _num("dwpc").alias("dewpoint_c"),
            _num("relh").alias("rel_humidity_pct"),
            _num("drct").alias("wind_dir_deg"),                    # variable direction (VRB) -> NULL
            _num("sknt").alias("wind_speed_kt"),
            _num("gust").alias("wind_gust_kt"),
            F.round(_num("vsby") * 1609.344, 0).alias("visibility_m"),    # statute miles -> m (9999 m = 6.21 mi)
            F.least(*[F.when(_code(f"skyc{i}").isin(*CEILING_COVERS), _num(f"skyl{i}")) for i in range(1, 5)])
             .alias("ceiling_ft"),                                 # lowest BKN / OVC / VV layer, NULL = no ceiling
            _code("wxcodes").alias("present_weather"),
            F.when(F.trim(F.col("metar")) != "M", F.trim(F.col("metar"))).alias("raw_metar"),
            *LINEAGE)
        .withColumn("obs_date", F.to_date("obs_time_utc"))
        .withColumn("obs_hour", F.hour("obs_time_utc"))
        # Present weather codes, token by token: "+TSRA BCFG", "-SHRA", "VCSH" (VC = in the vicinity: not counted)
        .withColumn("has_thunderstorm", wx.rlike(r"(^|\s)[-+]?TS"))
        .withColumn("has_precipitation", wx.rlike(r"(^|\s)[-+]?(TS|SH|FZ)?(RA|SN|DZ|PL|GR|GS|SG|UP)"))
        .withColumn("has_fog", wx.rlike(r"(^|\s)(MI|BC|PR|FZ)?FG"))           # fog only: mist (BR) / haze (HZ) not
        .withColumn("is_imc", F.coalesce(F.col("visibility_m") < weather.vmc_visibility_m, F.lit(False))
                    | F.coalesce(F.col("ceiling_ft") < weather.vmc_ceiling_ft, F.lit(False)))
        .withColumn("is_below_cat1", F.coalesce(F.col("visibility_m") < weather.cat1_visibility_m, F.lit(False))
                    | F.coalesce(F.col("ceiling_ft") < weather.cat1_ceiling_ft, F.lit(False)))
        .withColumn("is_strong_wind", F.coalesce(F.col("wind_speed_kt") >= weather.strong_wind_kt, F.lit(False))
                    | F.coalesce(F.col("wind_gust_kt") >= weather.strong_gust_kt, F.lit(False)))
        # unparsable times stay in scope, so that the quality rules quarantine them (as in silver.in_scope)
        .withColumn("_in_scope", F.col("obs_time_utc").isNull()
                    | ((F.col("obs_time_utc") >= scope_start) & (F.col("obs_time_utc") < scope_end))))


def flag_metar(obs):
    """Quarantine reason of each observation (first failing rule, NULL = valid)."""
    return obs.withColumn("_quarantine_reason",
        F.when(F.col("station_icao").isNull() | (F.length("station_icao") != 4), "MISSING_STATION")
        .when(F.col("obs_time_utc").isNull(), "UNPARSABLE_TIMESTAMP")
        .when((F.col("wind_speed_kt") < 0) | (F.col("wind_speed_kt") > 150) | (F.col("wind_gust_kt") < 0)
              | ~F.col("wind_dir_deg").between(0, 360), "INVALID_WIND")
        .when(F.col("visibility_m") < 0, "INVALID_VISIBILITY")
        .when(~F.col("temp_c").between(-60, 60), "INVALID_TEMPERATURE"))


def empty_bronze_metar(spark):
    """bronze/metar does not exist (no weather station configured or no file downloaded): empty input."""
    schema = ", ".join(f"{c} string" for c in (*METAR_COLUMNS, *LINEAGE))
    return spark.createDataFrame([], schema)


def build_silver(spark, settings):
    """silver/weather_observations and quarantine/weather_observations, with the row accounting of the weather."""
    bronze_path = settings.bronze_dir / "metar"
    available = bronze_path.exists() and any(bronze_path.rglob("*.parquet"))
    bronze = spark.read.parquet(str(bronze_path)) if available else empty_bronze_metar(spark)
    if not available:
        log.warning("No bronze/metar table: the weather tables are written empty (no station or no METAR file)")

    std = standardize_metar(bronze, settings.weather, settings.start, settings.end)
    flagged = flag_metar(std.filter(F.col("_in_scope")).drop("_in_scope"))
    valid = (flagged.filter(F.col("_quarantine_reason").isNull())
             .dropDuplicates(["station_icao", "obs_time_utc"])          # same station, same time = same report
             .withColumn("_silver_processed_at_utc", F.current_timestamp())
             .select(*SILVER_COLUMNS))
    quarantine = flagged.filter(F.col("_quarantine_reason").isNotNull())

    s, q = settings.silver_dir / "weather_observations", settings.quarantine_dir / "weather_observations"
    write_table(valid, s, files=1)
    write_table(quarantine, q, files=1)

    # Row accounting from the written tables: Bronze = out of scope + quarantined + duplicates + Silver
    silver_df, quar_df = spark.read.parquet(str(s)), spark.read.schema(quarantine.schema).parquet(str(q))
    b_n, out_n = bronze.count(), std.filter(~F.col("_in_scope")).count()
    q_n, s_n = quar_df.count(), silver_df.count()
    accounting = dict(bronze=b_n, out_of_scope=out_n, quarantined=q_n, duplicates=b_n - out_n - q_n - s_n, silver=s_n)
    reasons = {r[0]: r[1] for r in quar_df.groupBy("_quarantine_reason").count().collect()}
    stations = {r[0]: r[1] for r in silver_df.groupBy("station_icao").count().orderBy("station_icao").collect()}
    conditions = silver_df.agg(*[
        F.round(F.avg(F.col(c).cast("int")) * 100, 2).alias(f"pct_{c}")
        for c in ("has_thunderstorm", "has_precipitation", "has_fog", "is_imc", "is_below_cat1", "is_strong_wind")],
        F.round(F.avg(F.col("visibility_m").isNull().cast("int")) * 100, 2).alias("pct_visibility_missing"),
        F.round(F.avg(F.col("wind_speed_kt").isNull().cast("int")) * 100, 2).alias("pct_wind_missing"),
    ).first().asDict()
    missing = sorted(set(settings.weather.stations) - set(stations))
    log.info("Weather: %s observations of %d stations | accounting %s | quarantine %s", f"{s_n:,}", len(stations),
             accounting, reasons)
    if missing:
        log.warning("Weather: no observation for the configured station(s) %s", missing)
    return dict(available=s_n > 0, accounting=accounting, accounting_consistent=accounting["duplicates"] >= 0,
                quarantine_reasons=reasons, observations_by_station=stations, stations_without_data=missing,
                conditions=conditions)
