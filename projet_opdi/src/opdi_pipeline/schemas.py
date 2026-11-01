"""Schema contracts: the columns and types each table must have.

- RAW: schema of the OPDI v0.0.2 files of 2026. Bronze compares every raw file with it, so a schema change at the
  source (e.g. the `decimal` ids and `timestamp_ntz` dates of 2025) stops the run before Silver, instead of
  producing wrong data. A new variant then needs a mapping in silver.standardize_* and a new contract.
- SILVER, GOLD: schema of the published tables. Silver and Gold check their written tables against them, and the
  tests check them too, so that a code change cannot silently change what downstream consumers read.
"""

RAW = {
    "flight_list": {
        "id": "bigint", "icao24": "string", "flt_id": "string", "dof": "string", "adep": "string",
        "ades": "string", "adep_p": "string", "ades_p": "string", "registration": "string", "model": "string",
        "typecode": "string", "icao_aircraft_class": "string", "icao_operator": "string",
        "first_seen": "string", "last_seen": "string", "version": "string"},
    "flight_events": {
        "id": "bigint", "flight_id": "bigint", "type": "string", "event_time": "string", "longitude": "double",
        "latitude": "double", "altitude": "double", "source": "string", "version": "string", "info": "string"},
    "measurements": {
        "id": "bigint", "event_id": "bigint", "type": "string", "value": "double", "version": "string"},
}

SILVER = {
    "flights": {
        "flight_id": "bigint", "callsign": "string", "transponder_icao24": "string", "registration": "string",
        "aircraft_typecode": "string", "aircraft_model": "string", "aircraft_class": "string",
        "operator_icao": "string", "origin_icao": "string", "dest_icao": "string", "route": "string",
        "flight_category": "string", "origin_airport_name": "string", "origin_country": "string",
        "origin_municipality": "string", "origin_lat": "double", "origin_lon": "double",
        "dest_airport_name": "string", "dest_country": "string", "dest_municipality": "string",
        "dest_lat": "double", "dest_lon": "double", "distance_km": "double", "duration_min": "double",
        "first_seen_utc": "timestamp", "last_seen_utc": "timestamp", "flight_date": "date", "has_adep": "boolean",
        "has_ades": "boolean", "is_short_flight": "boolean", "_silver_processed_at_utc": "timestamp",
        "_source_file": "string", "_source_period": "int", "_ingestion_run_id": "string",
        "_source_sha256": "string", "flight_year": "int", "flight_month": "int"},
    "flight_events": {
        "event_id": "bigint", "flight_id": "bigint", "event_type": "string", "event_timestamp_utc": "timestamp",
        "event_date": "date", "latitude": "double", "longitude": "double", "altitude_ft": "double",
        "is_altitude_valid": "boolean", "source": "string", "info": "map<string,string>",
        "_silver_processed_at_utc": "timestamp", "_source_file": "string", "_ingestion_run_id": "string",
        "_source_sha256": "string", "_source_period": "int"},
    "airports": {
        "airport_icao": "string", "airport_name": "string", "airport_type": "string", "iso_country": "string",
        "continent": "string", "municipality": "string", "latitude": "double", "longitude": "double",
        "elevation_ft": "double", "iata_code": "string"},
    "runways": {
        "airport_icao": "string", "le_ident": "string", "he_ident": "string", "length_ft": "double",
        "width_ft": "double", "surface": "string", "is_closed": "boolean", "le_heading_deg": "double",
        "he_heading_deg": "double"},
}

GOLD = {
    "airport_hourly_ops": {
        "airport_icao": "string", "movement_date": "date", "movement_hour": "int", "departures": "bigint",
        "arrivals": "bigint", "movements": "bigint", "airport_name": "string", "iso_country": "string",
        "municipality": "string", "_gold_built_at_utc": "timestamp"},
    "airport_daily_ops": {
        "airport_icao": "string", "movement_date": "date", "departures": "bigint", "arrivals": "bigint",
        "movements": "bigint", "distinct_operators": "bigint", "distinct_aircraft_types": "bigint",
        "pct_jet": "double", "pct_international": "double", "pct_time_from_event": "double",
        "peak_hour_movements": "bigint", "airport_name": "string", "iso_country": "string",
        "municipality": "string", "day_of_week": "int", "_gold_built_at_utc": "timestamp"},
    "runway_usage": {
        "airport_icao": "string", "runway_ident": "string", "movement_date": "date", "runway_departures": "bigint",
        "runway_arrivals": "bigint", "runway_movements": "bigint", "length_ft": "double", "width_ft": "double",
        "surface": "string", "is_closed": "boolean", "_gold_built_at_utc": "timestamp"},
    "airport_ground_times": {
        "airport_icao": "string", "movement_date": "date", "total_departures": "bigint",
        "taxi_out_measured_flights": "bigint", "avg_taxi_out_min": "double", "median_taxi_out_min": "double",
        "p90_taxi_out_min": "double", "total_arrivals": "bigint", "taxi_in_measured_flights": "bigint",
        "avg_taxi_in_min": "double", "median_taxi_in_min": "double", "p90_taxi_in_min": "double",
        "airport_name": "string", "iso_country": "string", "municipality": "string",
        "pct_taxi_out_coverage": "double", "pct_taxi_in_coverage": "double", "_gold_built_at_utc": "timestamp"},
}


def as_dict(schema):
    """{column: type} of a Spark schema (StructType)."""
    return {f.name: f.dataType.simpleString() for f in schema.fields}


def compare(schema, expected):
    """Differences between a Spark schema (or a {column: type} dict) and its contract; empty list = conform."""
    actual = schema if isinstance(schema, dict) else as_dict(schema)
    problems = [f"missing column {c} ({t})" for c, t in expected.items() if c not in actual]
    problems += [f"unexpected column {c} ({t})" for c, t in actual.items() if c not in expected]
    problems += [f"column {c}: {actual[c]} instead of {t}" for c, t in expected.items()
                 if c in actual and actual[c] != t]
    return problems
