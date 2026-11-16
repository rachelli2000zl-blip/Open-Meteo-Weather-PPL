import hashlib
import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import duckdb

CATEGORIES = (
    "weather_forecast",
    "historical_weather",
    "ensemble_models",
    "seasonal_forecast",
    "climate_change",
    "marine_forecast",
    "air_quality",
    "satellite_radiation",
    "flood",
    "geocoding",
    "elevation",
)
PACKAGE = files("meteo_warehouse")


def now():
    return datetime.now(UTC)


def json_text(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def digest(value):
    return hashlib.sha256(json_text(value).encode()).hexdigest()


def key(value):
    return int(digest(value)[:15], 16)


def uuid_key(value):
    return uuid.uuid5(uuid.NAMESPACE_URL, "open-meteo-warehouse:" + digest(value))


def instant(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Expected a timestamp with an explicit UTC offset")
    return value.astimezone(UTC)


@contextmanager
def transaction(db):
    db.execute("BEGIN")
    try:
        yield
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def insert(db, table, values):
    db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in values)})", values)


def insert_many(db, table, rows):
    if rows:
        # DuckDB unnests parallel column lists side by side.
        columns = list(zip(*rows, strict=True))
        db.execute(
            f"INSERT INTO {table} SELECT {','.join('unnest(?)' for _ in columns)}", [list(column) for column in columns]
        )


def initialize(path):
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(path))
    try:
        db.execute("SET TimeZone = 'UTC'")
        db.execute(PACKAGE.joinpath("schema.sql").read_text())
        for category in CATEGORIES:
            create_bronze(db, category)
        db.execute(PACKAGE.joinpath("gold_schema.sql").read_text())
    except BaseException:
        db.close()
        raise
    return db


def create_bronze(db, category):
    parent = f"bronze.{category}"
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS {parent} (
            fetch_id UUID PRIMARY KEY, slice_id UUID NOT NULL,
            attempt_no INTEGER NOT NULL CHECK (attempt_no > 0), api_product VARCHAR NOT NULL,
            source_host VARCHAR NOT NULL, source_path VARCHAR NOT NULL, provider_code VARCHAR,
            requested_model_selector VARCHAR NOT NULL, selector_kind VARCHAR NOT NULL,
            request_params JSON NOT NULL, requested_run_at TIMESTAMPTZ, requested_at TIMESTAMPTZ NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL, http_status INTEGER, content_type VARCHAR,
            response_body BLOB, body_sha256 VARCHAR, fetch_error VARCHAR,
            estimated_call_units DECIMAL(12,3) NOT NULL,
            UNIQUE (slice_id, attempt_no)
        )
    """)
    if category in {"geocoding", "elevation"}:
        return
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS {parent}_point (
            fetch_id UUID REFERENCES {parent}(fetch_id), result_index INTEGER, request_index INTEGER NOT NULL,
            location_key VARCHAR NOT NULL, requested_location_name VARCHAR,
            requested_timezone VARCHAR, returned_timezone VARCHAR, timezone_abbreviation VARCHAR,
            result_json_path VARCHAR NOT NULL, utc_offset_seconds INTEGER,
            requested_latitude DOUBLE NOT NULL, requested_longitude DOUBLE NOT NULL,
            returned_latitude DOUBLE, returned_longitude DOUBLE, returned_elevation_m DOUBLE,
            generationtime_ms DOUBLE, PRIMARY KEY (fetch_id, result_index)
        );
        CREATE TABLE IF NOT EXISTS {parent}_block (
            fetch_id UUID, result_index INTEGER, block_code VARCHAR, sample_count INTEGER,
            current_interval_seconds INTEGER, data_json_path VARCHAR NOT NULL, units_json_path VARCHAR,
            first_time_raw VARCHAR, last_time_raw VARCHAR, returned_fields VARCHAR[] NOT NULL,
            PRIMARY KEY (fetch_id, result_index, block_code),
            FOREIGN KEY (fetch_id, result_index) REFERENCES {parent}_point(fetch_id, result_index)
        );
        CREATE OR REPLACE VIEW {parent}_inspection AS
        SELECT b.*, p.location_key, r.slice_id, r.api_product,
               json_extract(try_cast(decode(r.response_body, 'replace') AS JSON), b.data_json_path) AS block_json,
               json_extract(try_cast(decode(r.response_body, 'replace') AS JSON), b.units_json_path) AS units_json
        FROM {parent}_block b
        JOIN {parent}_point p USING (fetch_id, result_index)
        JOIN {parent} r USING (fetch_id)
    """)
