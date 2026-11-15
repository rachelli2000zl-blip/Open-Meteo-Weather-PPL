CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS ops.forecast_model (
    model_version_id BIGINT PRIMARY KEY, provider_code VARCHAR, model_code VARCHAR NOT NULL,
    domain_code VARCHAR NOT NULL, grid_version VARCHAR, spatial_resolution DOUBLE,
    resolution_unit VARCHAR, nominal_horizon_hours INTEGER, nominal_update_interval_seconds INTEGER,
    metadata_url VARCHAR, effective_from TIMESTAMPTZ, effective_to TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS ops.api_capability (
    capability_id BIGINT PRIMARY KEY, api_product VARCHAR NOT NULL,
    requested_model_selector VARCHAR NOT NULL, domain_code VARCHAR NOT NULL,
    block_code VARCHAR NOT NULL, api_field VARCHAR NOT NULL, request_limits JSON,
    data_horizon_hours INTEGER, native_step_seconds INTEGER, support_status VARCHAR NOT NULL,
    verified_at TIMESTAMPTZ, effective_from TIMESTAMPTZ, effective_to TIMESTAMPTZ,
    specification JSON NOT NULL
);
CREATE TABLE IF NOT EXISTS bronze.model_update (
    poll_id UUID PRIMARY KEY, model_version_id BIGINT NOT NULL,
    metadata_url VARCHAR NOT NULL, checked_at TIMESTAMPTZ NOT NULL, http_status INTEGER,
    response_body BLOB, body_sha256 VARCHAR, fetch_error VARCHAR
);
CREATE TABLE IF NOT EXISTS ops.extraction_slice (
    slice_id UUID PRIMARY KEY, slice_key VARCHAR UNIQUE NOT NULL, category VARCHAR NOT NULL,
    api_product VARCHAR NOT NULL, requested_model_selector VARCHAR NOT NULL,
    spatial_batch_key VARCHAR NOT NULL, profile_key VARCHAR NOT NULL, bundle_key VARCHAR NOT NULL,
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    poll_slot_at TIMESTAMPTZ, requested_run_at TIMESTAMPTZ,
    trigger_poll_id UUID, not_before_at TIMESTAMPTZ,
    expected_fields JSON NOT NULL, estimated_call_units DECIMAL(12,3) CHECK (estimated_call_units >= 1),
    status VARCHAR NOT NULL, coverage_result VARCHAR, priority INTEGER NOT NULL,
    specification JSON NOT NULL, CHECK (window_end > window_start)
);
CREATE TABLE IF NOT EXISTS ops.slice_capability (
    slice_id UUID REFERENCES ops.extraction_slice(slice_id),
    capability_id BIGINT REFERENCES ops.api_capability(capability_id),
    PRIMARY KEY (slice_id, capability_id)
);
CREATE TABLE IF NOT EXISTS ops.capability_evidence (
    slice_id UUID, capability_id BIGINT, fetch_id UUID, location_key VARCHAR, observed_at TIMESTAMPTZ NOT NULL,
    usable_slots INTEGER NOT NULL, usable_members INTEGER,
    PRIMARY KEY (slice_id, capability_id, fetch_id, location_key),
    FOREIGN KEY (slice_id, capability_id) REFERENCES ops.slice_capability(slice_id, capability_id)
);
CREATE OR REPLACE VIEW ops.verified_capability AS
SELECT c.*, e.observed_at, e.locations
FROM ops.api_capability c JOIN (
    SELECT capability_id, max(observed_at) AS observed_at, count(DISTINCT location_key) AS locations
    FROM ops.capability_evidence GROUP BY capability_id
) e USING (capability_id);
CREATE TABLE IF NOT EXISTS ops.call_reservation (
    fetch_id UUID PRIMARY KEY, reserved_at TIMESTAMPTZ NOT NULL,
    call_units DECIMAL(12,3) NOT NULL CHECK (call_units >= 0), is_metadata BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS ops.quality_issue (
    issue_key VARCHAR PRIMARY KEY, slice_id UUID NOT NULL REFERENCES ops.extraction_slice(slice_id),
    fetch_id UUID, location_key VARCHAR, block_code VARCHAR, api_field VARCHAR,
    window_start TIMESTAMPTZ, window_end TIMESTAMPTZ, rule_code VARCHAR NOT NULL,
    disposition VARCHAR NOT NULL, detail JSON
);

CREATE TABLE IF NOT EXISTS silver.product (
    product_id BIGINT PRIMARY KEY, bronze_category VARCHAR NOT NULL,
    api_product VARCHAR UNIQUE NOT NULL, data_kind VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.place (
    place_id BIGINT PRIMARY KEY, latitude DOUBLE NOT NULL, longitude DOUBLE NOT NULL,
    country_code VARCHAR, timezone VARCHAR
);
CREATE TABLE IF NOT EXISTS silver.place_name (
    place_id BIGINT REFERENCES silver.place(place_id), language VARCHAR, place_name VARCHAR,
    PRIMARY KEY (place_id, language, place_name)
);
CREATE TABLE IF NOT EXISTS silver.site (
    site_id BIGINT PRIMARY KEY, location_key VARCHAR UNIQUE NOT NULL,
    latitude DOUBLE NOT NULL, longitude DOUBLE NOT NULL, display_name VARCHAR,
    place_id BIGINT REFERENCES silver.place(place_id)
);
CREATE TABLE IF NOT EXISTS silver.source_point (
    source_point_id BIGINT PRIMARY KEY, latitude DOUBLE NOT NULL, longitude DOUBLE NOT NULL,
    elevation_m DOUBLE, grid_version VARCHAR
);
CREATE TABLE IF NOT EXISTS silver.measure (
    measure_id BIGINT PRIMARY KEY, quantity_code VARCHAR NOT NULL, vertical_kind VARCHAR NOT NULL,
    level_from DOUBLE, level_to DOUBLE, level_unit VARCHAR, temporal_statistic VARCHAR NOT NULL,
    canonical_unit VARCHAR NOT NULL, value_type VARCHAR NOT NULL,
    definition_version INTEGER NOT NULL, measure_code VARCHAR NOT NULL,
    UNIQUE (measure_code, definition_version)
);
CREATE TABLE IF NOT EXISTS silver.field_mapping (
    mapping_id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL REFERENCES silver.product(product_id),
    selector_scope VARCHAR NOT NULL, block_code VARCHAR NOT NULL, api_field VARCHAR NOT NULL,
    measure_id BIGINT NOT NULL REFERENCES silver.measure(measure_id), time_window_rule VARCHAR NOT NULL,
    mapping_version INTEGER NOT NULL, semantics JSON NOT NULL,
    UNIQUE (product_id, selector_scope, block_code, api_field, mapping_version)
);
CREATE TABLE IF NOT EXISTS silver.request_profile (
    profile_id BIGINT PRIMARY KEY, cell_selection VARCHAR, elevation_mode VARCHAR NOT NULL,
    elevation_override_m DOUBLE, aggregation_timezone VARCHAR NOT NULL,
    solar_tilt DOUBLE, solar_azimuth DOUBLE, options_json JSON NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.series (
    series_id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL REFERENCES silver.product(product_id),
    site_id BIGINT NOT NULL REFERENCES silver.site(site_id),
    measure_id BIGINT NOT NULL REFERENCES silver.measure(measure_id),
    profile_id BIGINT NOT NULL REFERENCES silver.request_profile(profile_id),
    requested_model_selector VARCHAR NOT NULL, block_code VARCHAR NOT NULL, output_cadence INTEGER,
    member_kind VARCHAR NOT NULL, member_number INTEGER, ensemble_statistic VARCHAR,
    ensemble_percentile DOUBLE, scenario_code VARCHAR, fixed_lead_hours INTEGER,
    identity_key VARCHAR UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.vintage (
    vintage_id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL REFERENCES silver.product(product_id),
    vintage_kind VARCHAR NOT NULL CHECK (vintage_kind IN ('exact_run', 'capture', 'archive')),
    source_key VARCHAR NOT NULL, initialized_at TIMESTAMPTZ, captured_at TIMESTAMPTZ,
    UNIQUE (product_id, vintage_kind, source_key)
);
CREATE TABLE IF NOT EXISTS silver.series_value (
    series_id BIGINT REFERENCES silver.series(series_id),
    vintage_id BIGINT REFERENCES silver.vintage(vintage_id), valid_at TIMESTAMPTZ,
    revision_no INTEGER CHECK (revision_no > 0),
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    source_available_at TIMESTAMPTZ, warehouse_available_at TIMESTAMPTZ NOT NULL,
    source_date DATE NOT NULL, source_timezone VARCHAR NOT NULL, source_utc_offset_seconds INTEGER NOT NULL,
    value_number DOUBLE, value_code INTEGER, value_timestamp TIMESTAMPTZ, value_boolean BOOLEAN, value_text VARCHAR,
    source_unit VARCHAR NOT NULL, quality_status VARCHAR NOT NULL, sampling_origin VARCHAR NOT NULL,
    source_point_id BIGINT REFERENCES silver.source_point(source_point_id),
    source_model_version_id BIGINT,
    mapping_id BIGINT NOT NULL REFERENCES silver.field_mapping(mapping_id),
    fetch_id UUID NOT NULL, result_index INTEGER NOT NULL, block_code VARCHAR NOT NULL,
    api_field VARCHAR NOT NULL, sample_index INTEGER NOT NULL,
    PRIMARY KEY (series_id, vintage_id, valid_at, revision_no),
    CHECK (window_start <= valid_at AND valid_at <= window_end),
    CHECK ((value_number IS NOT NULL)::INTEGER + (value_code IS NOT NULL)::INTEGER +
           (value_timestamp IS NOT NULL)::INTEGER + (value_boolean IS NOT NULL)::INTEGER +
           (value_text IS NOT NULL)::INTEGER = CASE WHEN quality_status = 'source_null' THEN 0 ELSE 1 END)
);
CREATE TABLE IF NOT EXISTS silver.series_value_source (
    fetch_id UUID, result_index INTEGER, block_code VARCHAR, api_field VARCHAR, sample_index INTEGER,
    mapping_id BIGINT REFERENCES silver.field_mapping(mapping_id),
    series_id BIGINT NOT NULL, vintage_id BIGINT NOT NULL, valid_at TIMESTAMPTZ NOT NULL, revision_no INTEGER NOT NULL,
    PRIMARY KEY (fetch_id, result_index, block_code, api_field, sample_index, mapping_id),
    FOREIGN KEY (series_id, vintage_id, valid_at, revision_no)
        REFERENCES silver.series_value(series_id, vintage_id, valid_at, revision_no)
);
CREATE TABLE IF NOT EXISTS silver.coverage (
    slice_id UUID, location_key VARCHAR, block_code VARCHAR,
    api_field VARCHAR, window_start TIMESTAMPTZ, window_end TIMESTAMPTZ, fetch_id UUID,
    coverage_status VARCHAR NOT NULL, expected_slots INTEGER, returned_slots INTEGER NOT NULL,
    usable_slots INTEGER NOT NULL, explicit_null_slots INTEGER NOT NULL, invalid_slots INTEGER NOT NULL,
    missing_slots INTEGER, expected_members INTEGER, usable_members INTEGER,
    PRIMARY KEY (slice_id, location_key, block_code, api_field, window_start, window_end)
);
CREATE TABLE IF NOT EXISTS silver.model_update_observation (
    poll_id UUID PRIMARY KEY, initialized_at TIMESTAMPTZ NOT NULL,
    modified_at TIMESTAMPTZ, source_available_at TIMESTAMPTZ NOT NULL,
    temporal_resolution_seconds INTEGER, update_interval_seconds INTEGER
);

CREATE TABLE IF NOT EXISTS ops.availability_evidence (
    fetch_id UUID PRIMARY KEY, source_available_at TIMESTAMPTZ NOT NULL,
    source_reference VARCHAR NOT NULL, ordered_correction BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.ingestion_batch (
    batch_id UUID PRIMARY KEY, phase VARCHAR NOT NULL, started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL, attempts INTEGER NOT NULL, usable_attempts INTEGER NOT NULL,
    new_facts BIGINT NOT NULL, CHECK (finished_at >= started_at)
);
