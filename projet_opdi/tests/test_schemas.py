"""Schema contracts: the comparison itself, the raw contract, and the stop on a schema change at the source."""
import pyarrow as pa
import pyarrow.parquet as pq

from opdi_pipeline import bronze, schemas
from opdi_pipeline.config import load_settings
from synthetic_opdi import EV_SCHEMA, FL_SCHEMA, MS_SCHEMA


def test_compare_reports_every_kind_of_difference():
    expected = {"id": "bigint", "name": "string", "value": "double"}
    assert schemas.compare({"id": "bigint", "name": "string", "value": "double"}, expected) == []
    problems = schemas.compare({"id": "decimal(20,0)", "name": "string", "extra": "int"}, expected)
    assert "missing column value (double)" in problems
    assert "unexpected column extra (int)" in problems
    assert "column id: decimal(20,0) instead of bigint" in problems


def test_raw_contract_is_the_opdi_2026_schema():
    # the synthetic test files use the real 2026 schemas: they must match the contract
    for table, arrow_schema in [("flight_list", FL_SCHEMA), ("flight_events", EV_SCHEMA), ("measurements", MS_SCHEMA)]:
        assert list(schemas.RAW[table]) == arrow_schema.names


def test_published_tables_have_a_contract():
    assert set(schemas.SILVER) == {"flights", "flight_events", "airports", "runways"}
    assert set(schemas.GOLD) == {"airport_hourly_ops", "airport_daily_ops", "runway_usage", "airport_ground_times"}


def test_schema_change_at_the_source_is_detected(spark, tmp_path):
    """A raw file with the 2025 schema (decimal ids, timestamps, extra column) does not match the contract."""
    settings = load_settings("dev", data_root=str(tmp_path))
    (settings.raw_dir / "measurements").mkdir(parents=True)
    old_schema = pa.schema([("id", pa.decimal128(20, 0)), ("event_id", pa.string()), ("type", pa.string()),
                            ("value", pa.float64()), ("version", pa.string()), ("unix_time", pa.int64())])
    pq.write_table(pa.Table.from_pylist([], schema=old_schema),
                   settings.raw_dir / "measurements" / "measurements_20250601_20250611.parquet")
    _, rows = bronze.check_schemas(spark, settings)
    assert rows[0]["matches_contract"] is False
