CREATE TABLE IF NOT EXISTS bronze.geocoding_result (
    fetch_id UUID REFERENCES bronze.geocoding(fetch_id), result_index INTEGER, place_id BIGINT,
    latitude DOUBLE, longitude DOUBLE, place_name VARCHAR, requested_language VARCHAR,
    country_code VARCHAR, timezone VARCHAR, result_json_path VARCHAR,
    PRIMARY KEY (fetch_id, result_index)
);
CREATE TABLE IF NOT EXISTS bronze.elevation_result (
    fetch_id UUID REFERENCES bronze.elevation(fetch_id), request_index INTEGER, result_json_path VARCHAR,
    requested_latitude DOUBLE, requested_longitude DOUBLE, elevation_m DOUBLE,
    PRIMARY KEY (fetch_id, request_index)
);
CREATE TABLE IF NOT EXISTS silver.geocode_match (
    fetch_id UUID, result_index INTEGER, place_id BIGINT NOT NULL REFERENCES silver.place(place_id),
    PRIMARY KEY (fetch_id, result_index)
);
CREATE TABLE IF NOT EXISTS silver.elevation (
    site_id BIGINT REFERENCES silver.site(site_id), fetch_id UUID, request_index INTEGER,
    elevation_m DOUBLE NOT NULL, warehouse_available_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (site_id, fetch_id, request_index)
);

CREATE TABLE IF NOT EXISTS ops.gold_contract (
    contract_name VARCHAR, contract_version VARCHAR, selection_rules JSON NOT NULL,
    sql_sha256 VARCHAR NOT NULL, PRIMARY KEY (contract_name, contract_version)
);
CREATE TABLE IF NOT EXISTS ops.gold_build (
    build_id VARCHAR PRIMARY KEY, contract_name VARCHAR NOT NULL, contract_version VARCHAR NOT NULL,
    input_watermark TIMESTAMPTZ NOT NULL, code_version VARCHAR NOT NULL, config_version VARCHAR NOT NULL,
    affected_scopes JSON NOT NULL, row_counts JSON NOT NULL, quality_results JSON NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (contract_name, contract_version) REFERENCES ops.gold_contract(contract_name, contract_version)
);
CREATE TABLE IF NOT EXISTS ops.gold_build_input (
    build_id VARCHAR REFERENCES ops.gold_build(build_id), series_id BIGINT, vintage_id BIGINT,
    valid_at TIMESTAMPTZ, revision_no INTEGER,
    PRIMARY KEY (build_id, series_id, vintage_id, valid_at, revision_no)
);
CREATE TABLE IF NOT EXISTS gold.forecast_release (
    release_id VARCHAR PRIMARY KEY, contract_version VARCHAR NOT NULL,
    site_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    requested_model_selector VARCHAR NOT NULL, profile_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    vintage_id BIGINT NOT NULL, initialized_at TIMESTAMPTZ,
    required_inputs_available_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ NOT NULL,
    coverage_status VARCHAR NOT NULL, build_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.release_input (
    release_id VARCHAR REFERENCES gold.forecast_release(release_id),
    series_id BIGINT, vintage_id BIGINT, valid_at TIMESTAMPTZ, revision_no INTEGER,
    PRIMARY KEY (release_id, series_id, vintage_id, valid_at, revision_no)
);
CREATE TABLE IF NOT EXISTS gold.forecast_hourly_latest (
    contract_version VARCHAR, site_id BIGINT, product_id BIGINT, requested_model_selector VARCHAR, profile_id BIGINT,
    valid_at TIMESTAMPTZ, release_id VARCHAR NOT NULL REFERENCES gold.forecast_release(release_id),
    temp_2m_c DOUBLE, precip_mm DOUBLE, wind_10m_ms DOUBLE, coverage_status VARCHAR NOT NULL,
    PRIMARY KEY (contract_version, site_id, product_id, requested_model_selector, profile_id, valid_at)
);
CREATE TABLE IF NOT EXISTS gold.weather_daily (
    contract_version VARCHAR, site_id BIGINT, product_id BIGINT, requested_model_selector VARCHAR, profile_id BIGINT,
    reporting_timezone VARCHAR, local_date DATE, window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    temp_min_c DOUBLE, temp_max_c DOUBLE, precip_mm DOUBLE, wind_gust_max_ms DOUBLE,
    metric_coverage JSON NOT NULL, build_id VARCHAR NOT NULL,
    PRIMARY KEY (contract_version, site_id, product_id, requested_model_selector, profile_id, reporting_timezone, local_date)
);
CREATE TABLE IF NOT EXISTS gold.environment_daily (
    contract_version VARCHAR, site_id BIGINT, product_id BIGINT, requested_model_selector VARCHAR, profile_id BIGINT,
    reporting_timezone VARCHAR, local_date DATE, metric_code VARCHAR,
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL, value_number DOUBLE, unit VARCHAR NOT NULL,
    expected_slots INTEGER NOT NULL, usable_slots INTEGER NOT NULL, coverage_status VARCHAR NOT NULL,
    build_id VARCHAR NOT NULL,
    PRIMARY KEY (contract_version, site_id, product_id, requested_model_selector, profile_id,
                 reporting_timezone, local_date, metric_code)
);
CREATE TABLE IF NOT EXISTS gold.ensemble_hourly (
    release_id VARCHAR REFERENCES gold.forecast_release(release_id), measure_id BIGINT,
    valid_at TIMESTAMPTZ, p10 DOUBLE, p50 DOUBLE, p90 DOUBLE, expected_members INTEGER,
    usable_members INTEGER NOT NULL, coverage_status VARCHAR NOT NULL,
    PRIMARY KEY (release_id, measure_id, valid_at)
);
CREATE TABLE IF NOT EXISTS gold.ml_dataset (
    dataset_id VARCHAR PRIMARY KEY, feature_set_version VARCHAR NOT NULL, label_set_version VARCHAR NOT NULL,
    availability_basis VARCHAR NOT NULL, feature_surface VARCHAR NOT NULL,
    build_cutoff_at TIMESTAMPTZ NOT NULL, build_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.ml_forecast_features (
    dataset_id VARCHAR REFERENCES gold.ml_dataset(dataset_id),
    site_id BIGINT, product_id BIGINT, requested_model_selector VARCHAR, profile_id BIGINT,
    decision_at TIMESTAMPTZ, valid_at TIMESTAMPTZ, vintage_id BIGINT,
    release_id VARCHAR REFERENCES gold.forecast_release(release_id), initialized_at TIMESTAMPTZ,
    feature_available_at TIMESTAMPTZ, lead_hours DOUBLE, horizon_hours DOUBLE NOT NULL,
    forecast_temp_c DOUBLE, forecast_precip_mm DOUBLE, missing_required_features BOOLEAN NOT NULL,
    PRIMARY KEY (dataset_id, site_id, product_id, requested_model_selector, profile_id, decision_at, valid_at)
);
ALTER TABLE gold.ml_forecast_features ADD COLUMN IF NOT EXISTS rolling_features JSON;
ALTER TABLE gold.ml_forecast_features ADD COLUMN IF NOT EXISTS missing_features JSON;
CREATE TABLE IF NOT EXISTS gold.ml_weather_labels (
    dataset_id VARCHAR REFERENCES gold.ml_dataset(dataset_id), site_id BIGINT, valid_at TIMESTAMPTZ,
    target_temp_c DOUBLE, reference_product_id BIGINT NOT NULL, reference_profile_id BIGINT NOT NULL,
    label_available_at TIMESTAMPTZ, series_id BIGINT, vintage_id BIGINT, revision_no INTEGER,
    label_status VARCHAR NOT NULL, PRIMARY KEY (dataset_id, site_id, valid_at)
);
CREATE OR REPLACE VIEW gold.series_values AS
SELECT f.*, p.api_product, p.bronze_category, p.data_kind, s.site_id, t.location_key,
       t.display_name, t.latitude AS site_latitude, t.longitude AS site_longitude,
       sp.latitude AS source_latitude, sp.longitude AS source_longitude, sp.elevation_m AS source_elevation_m,
       s.measure_id, m.measure_code, m.definition_version, m.quantity_code, m.vertical_kind, m.level_from,
       m.level_to, m.level_unit, m.temporal_statistic, m.canonical_unit, m.value_type,
       s.product_id, s.profile_id, s.requested_model_selector, s.output_cadence, s.member_kind, s.member_number,
       s.ensemble_statistic, s.ensemble_percentile, s.scenario_code, s.fixed_lead_hours,
       rp.aggregation_timezone, rp.options_json, rp.cell_selection, rp.elevation_mode, rp.elevation_override_m,
       rp.solar_tilt, rp.solar_azimuth, v.vintage_kind, v.initialized_at, v.captured_at,
       epoch(f.valid_at - v.initialized_at) / 3600 AS lead_hours
FROM silver.series_value f
JOIN silver.series s USING (series_id)
JOIN silver.product p USING (product_id)
JOIN silver.site t USING (site_id)
JOIN silver.measure m USING (measure_id)
JOIN silver.request_profile rp USING (profile_id)
JOIN silver.vintage v USING (vintage_id)
LEFT JOIN silver.source_point sp USING (source_point_id);

CREATE OR REPLACE VIEW gold.forecast_temperature_validation AS
WITH selected_releases AS (
    SELECT r.*
    FROM gold.forecast_release r
    JOIN ops.gold_build b USING (build_id)
    WHERE b.contract_name = 'single_run_forecast' AND r.contract_version = 'v2'
      AND r.initialized_at IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY r.site_id, r.product_id, r.requested_model_selector, r.profile_id, r.initialized_at
        ORDER BY r.published_at DESC, r.release_id DESC
    ) = 1
), forecasts AS (
    SELECT r.release_id, r.site_id, r.profile_id, f.location_key,
           r.requested_model_selector AS forecast_model, r.initialized_at,
           r.published_at AS forecast_published_at, i.valid_at,
           epoch(i.valid_at - r.initialized_at) / 3600 AS lead_hours,
           f.value_number AS forecast_temp_c
    FROM selected_releases r
    JOIN gold.release_input i USING (release_id)
    JOIN gold.series_values f
      ON (f.series_id, f.vintage_id, f.valid_at, f.revision_no) =
         (i.series_id, i.vintage_id, i.valid_at, i.revision_no)
    WHERE f.measure_code = 'temperature_2m' AND f.definition_version = 1
      AND f.canonical_unit = '°C' AND f.block_code = 'hourly'
      AND i.valid_at >= r.window_start AND i.valid_at < r.window_end
), reference_revisions AS (
    SELECT f.site_id, f.profile_id, f.valid_at, f.series_id, f.vintage_id, f.revision_no,
           f.value_number, f.quality_status, f.window_start, f.window_end, f.warehouse_available_at
    FROM gold.series_values f
    JOIN (SELECT DISTINCT site_id, profile_id, valid_at FROM forecasts) t
      USING (site_id, profile_id, valid_at)
    WHERE f.api_product = 'archive' AND f.requested_model_selector = 'era5'
      AND f.measure_code = 'temperature_2m' AND f.definition_version = 1
      AND f.canonical_unit = '°C' AND f.block_code = 'hourly' AND f.output_cadence = 3600
      AND f.member_kind = 'deterministic' AND f.vintage_kind = 'archive'
      AND f.scenario_code IS NULL AND f.fixed_lead_hours IS NULL
    QUALIFY row_number() OVER (
        PARTITION BY f.series_id, f.vintage_id, f.valid_at ORDER BY f.revision_no DESC
    ) = 1
), reference_values AS (
    SELECT site_id, profile_id, valid_at,
           CASE WHEN count(*) = 1 AND bool_and(
               quality_status = 'valid' AND value_number IS NOT NULL
               AND window_start = valid_at AND window_end = valid_at
           ) THEN max(value_number) END AS reference_temp_c,
           CASE WHEN count(*) > 1 THEN 'ambiguous'
                WHEN bool_and(quality_status = 'valid' AND value_number IS NOT NULL
                              AND window_start = valid_at AND window_end = valid_at) THEN 'complete'
                ELSE 'unusable' END AS reference_status,
           CASE WHEN count(*) = 1 THEN max(series_id) END AS reference_series_id,
           CASE WHEN count(*) = 1 THEN max(vintage_id) END AS reference_vintage_id,
           CASE WHEN count(*) = 1 THEN max(revision_no) END AS reference_revision_no,
           CASE WHEN count(*) = 1 THEN max(warehouse_available_at) END AS reference_loaded_at
    FROM reference_revisions
    GROUP BY site_id, profile_id, valid_at
)
SELECT f.*, r.reference_temp_c, f.forecast_temp_c - r.reference_temp_c AS error_c,
       abs(f.forecast_temp_c - r.reference_temp_c) AS absolute_error_c,
       coalesce(r.reference_status, 'missing') AS reference_status,
       r.reference_series_id, r.reference_vintage_id, r.reference_revision_no, r.reference_loaded_at
FROM forecasts f
LEFT JOIN reference_values r USING (site_id, profile_id, valid_at);

CREATE TABLE IF NOT EXISTS ops.gold_refresh (
    contract_name VARCHAR, contract_version VARCHAR, processed_through TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (contract_name, contract_version),
    FOREIGN KEY (contract_name, contract_version) REFERENCES ops.gold_contract(contract_name, contract_version)
);
CREATE OR REPLACE VIEW gold.forecast_latest_releases AS
SELECT r.*, v.captured_at, epoch(current_timestamp - coalesce(r.initialized_at, v.captured_at)) AS age_seconds
FROM gold.forecast_release r JOIN silver.vintage v USING (vintage_id)
WHERE EXISTS (SELECT 1 FROM gold.forecast_hourly_latest h WHERE h.release_id = r.release_id);
