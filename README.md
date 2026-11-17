# Open-Meteo warehouse

The pipeline implements the Bronze, Silver, and Gold layers in [WAREHOUSE_DESIGN.md](WAREHOUSE_DESIGN.md). Python plans and fetches extraction slices. DuckDB stores raw attempts, validated series and revisions, reporting tables, and frozen training datasets.

The [baseline-v2 inventory](src/meteo_warehouse/starter.toml) configures all 11 Bronze categories. It covers Berlin weather, air quality and solar irradiance, marine data off Helgoland, and river discharge in Oslo. ERA5 reference weather covers all three sites for calendar years 2023–2025. Specialized products have their own dates, models and horizons in the inventory. `utc_hourly` and `berlin_calendar` are reusable request profiles. The baseline retains raw evidence, revisions and published manifests. Set your own sites, fields and dates through TOML. [Open-Meteo API catalog](https://open-meteo.com/en/docs)

## Analyze with the DuckDB browser UI

**1. Start DuckDB from Terminal.** From the project directory, copy and paste this entire block into zsh or bash. It uses Python 3.12 or later, initializes the warehouse, and opens the DuckDB browser UI with `weather.duckdb` attached as `warehouse`.

```bash
([ -x .venv/bin/python ] || python3 -m venv .venv) &&
  source .venv/bin/activate &&
  ([ -x .venv/bin/weather-warehouse ] || python -m pip install -e .) &&
  weather-warehouse init &&
  python - <<'PYTHON'
from pathlib import Path
from threading import Event

import duckdb

warehouse = Path("weather.duckdb").resolve(strict=True)
warehouse_path = str(warehouse).replace("'", "''")
Path("data").mkdir(exist_ok=True)

with duckdb.connect("data/analysis-ui.duckdb") as db:
    db.execute(f"ATTACH '{warehouse_path}' AS warehouse (READ_ONLY)")
    db.execute("SET GLOBAL TimeZone = 'UTC'")
    db.execute("CALL start_ui()")
    print("DuckDB UI: http://localhost:4213. Press Ctrl+C to stop.", flush=True)
    try:
        Event().wait()
    except KeyboardInterrupt:
        pass
PYTHON
```

Keep Terminal open. If the browser tab does not open automatically, go to [localhost:4213](http://localhost:4213). The UI stores notebooks in `data/analysis-ui.duckdb` and the warehouse is read-only in the browser session. The UI needs internet access to download its extension and frontend. [DuckDB UI documentation](https://duckdb.org/docs/1.5/core_extensions/ui)

**2. Run SQL in the browser.** Create a notebook, paste this query into a SQL cell, and click **Run**. The startup block has already attached the warehouse and set the query timezone to UTC.

```sql
SELECT local_date, temp_min_c, temp_max_c, precip_mm, wind_gust_max_ms
FROM warehouse.gold.weather_daily
WHERE contract_version = 'v2'
ORDER BY local_date DESC
LIMIT 10;
```

A fresh warehouse has empty report tables. To populate them, press **Ctrl+C** in Terminal to stop the UI, run the Terminal commands for your case below, then rerun the startup block. To query while the pipeline writes, attach a published [snapshot](#snapshots-and-benchmarks).

## Gold reporting and ML runbook

### Requirements

| # | Use case | What the Gold output provides |
| --- | --- | --- |
| 1 | **Berlin historical weather report** (`weather_daily`) | One row per local day for 2023–2025, with minimum and maximum temperature, total precipitation, and maximum wind gust. The report accounts for 23- and 25-hour days during daylight saving transitions. |
| 2 | **Helgoland historical weather report** (`weather_daily_helgoland`) | The same four daily metrics for Helgoland for 2023–2025. |
| 3 | **Oslo historical weather report** (`weather_daily_oslo`) | The same four daily metrics for Oslo for 2023–2025, using the `Europe/Oslo` calendar. |
| 4 | **Berlin live 24-hour forecast** (`forecast`) | One row per hour with temperature, precipitation, and wind speed. A new forecast is published when all three variables are available for all 24 hours from the same capture. |
| 5 | **Berlin forecast from a specific model run** (`single_run_forecast`) | A 24-hour forecast from a specified DWD ICON-D2 initialization time, with hourly temperature, precipitation, and wind speed. |
| 6 | **Berlin ensemble temperature forecast** (`ensemble`) | Hourly P10, P50, and P90 temperatures from 20 ensemble members, with coverage recorded when members are missing. |
| 7 | **Berlin ERA5 daily temperature report** (`weather_provider_daily`) | Daily minimum and maximum temperatures taken directly from ERA5, with checks that the source interval matches the reporting day. |
| 8 | **Berlin daily air quality report** (`environment_daily`) | Daily average PM2.5 derived from hourly values, with a record of data coverage. |
| 9 | **Berlin historical forecast validation** (`forecast_temperature_validation`) | Hourly temperature predictions from a selected historical model run, matched ERA5 reference temperatures, errors, reference coverage, and MAE, RMSE and bias. |

Run the shell blocks in Terminal from the project directory with the virtual environment active. Paste the SQL blocks into notebook cells after restarting the UI.

### Cases 1–3 and 7. Historical weather and ERA5 daily temperatures

The shared `archive` and `archive_daily` jobs cover Berlin, Helgoland, and Oslo. The starter hourly window includes the boundary hours needed to calculate precipitation and gusts through the final local day of 2025.

```bash
weather-warehouse plan archive
weather-warehouse plan archive_daily
weather-warehouse work --max-requests 200
weather-warehouse status
```

The two jobs plan 158 slices in total. Rerun `work` as needed for queued or deferred slices and retries that have reached their `not_before_at` time. After the fetches finish, publish the reports. The end date is exclusive.

```bash
weather-warehouse publish weather_daily           --start 2023-01-01 --end 2026-01-01
weather-warehouse publish weather_daily_helgoland --start 2023-01-01 --end 2026-01-01
weather-warehouse publish weather_daily_oslo      --start 2023-01-01 --end 2026-01-01
weather-warehouse publish weather_provider_daily  --start 2023-01-01 --end 2026-01-01
```

All four contracts write to `gold.weather_daily`. Select a single `b.contract_name` to retrieve one report. `day_hours` shows the length of the local day. For case 7, temperature fields are `NULL` when the ERA5 source interval fails the local-day check. `metric_coverage` records the source and completeness of each metric.

```sql
SELECT
    b.contract_name,
    s.location_key,
    d.local_date,
    d.reporting_timezone,
    epoch(d.window_end - d.window_start) / 3600 AS day_hours,
    d.temp_min_c,
    d.temp_max_c,
    d.precip_mm,
    d.wind_gust_max_ms,
    d.metric_coverage
FROM warehouse.gold.weather_daily AS d
JOIN warehouse.ops.gold_build AS b USING (build_id)
JOIN warehouse.silver.site AS s USING (site_id)
WHERE b.contract_name IN (
    'weather_daily',
    'weather_daily_helgoland',
    'weather_daily_oslo',
    'weather_provider_daily'
)
  AND d.contract_version IN ('v2', 'provider-v2')
  AND d.local_date >= DATE '2023-01-01'
  AND d.local_date <  DATE '2026-01-01'
ORDER BY b.contract_name, d.local_date;
```

### Case 4. Berlin live 24-hour forecast

```bash
weather-warehouse plan forecast
weather-warehouse work && weather-warehouse refresh forecast
```

Gold publishes when one capture supplies all three variables for all 24 hours. If the new window is `ineligible`, the query still returns the last published release if one exists. Check `published_at` for its publication time.

```sql
SELECT
    h.valid_at,
    h.temp_2m_c,
    h.precip_mm,
    h.wind_10m_ms,
    r.release_id,
    r.published_at
FROM warehouse.gold.forecast_hourly_latest AS h
JOIN warehouse.gold.forecast_release AS r USING (release_id)
JOIN warehouse.ops.gold_build AS b ON b.build_id = r.build_id
WHERE b.contract_name = 'forecast'
  AND h.contract_version = 'v2'
ORDER BY h.valid_at;
```

### Case 5. Berlin forecast from a specified ICON-D2 run

Set `RUN` to the initialization time you want and use the same timestamp in the SQL filter. The starter contract publishes lead hours 1 through 24. [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api)

```bash
RUN=2026-11-10T00:00:00Z

read -r RUN_START RUN_END < <(python - "$RUN" <<'PY'
import sys
from datetime import UTC, datetime, timedelta

run = datetime.fromisoformat(sys.argv[1]).astimezone(UTC)
print((run + timedelta(hours=1)).isoformat(),
      (run + timedelta(hours=25)).isoformat())
PY
)

weather-warehouse plan single_runs --run "$RUN"
weather-warehouse work
weather-warehouse publish single_run_forecast --start "$RUN_START" --end "$RUN_END"
```

The query reads the pinned inputs for a published run, including after a newer run takes over the latest table. If publication returns `superseded` and the requested run has no previous release, insert `--db data/icon-d2-run.duckdb` before the subcommand in each of the three `weather-warehouse` commands. In a notebook cell, run `DETACH warehouse;` and then `ATTACH 'data/icon-d2-run.duckdb' AS warehouse (READ_ONLY);`.

```sql
WITH selected_run AS (
    SELECT r.release_id, r.initialized_at
    FROM warehouse.gold.forecast_release AS r
    JOIN warehouse.ops.gold_build AS b USING (build_id)
    WHERE b.contract_name = 'single_run_forecast'
      AND r.initialized_at = TIMESTAMPTZ '2026-11-10 00:00:00+00'
    ORDER BY r.published_at DESC, r.release_id DESC
    LIMIT 1
)
SELECT
    r.initialized_at,
    i.valid_at,
    max(v.value_number) FILTER (WHERE v.measure_code = 'temperature_2m') AS temp_2m_c,
    max(v.value_number) FILTER (WHERE v.measure_code = 'precipitation') AS precip_mm,
    max(v.value_number) FILTER (WHERE v.measure_code = 'wind_speed_10m') AS wind_10m_ms
FROM warehouse.gold.release_input AS i
JOIN warehouse.gold.series_values AS v USING (series_id, vintage_id, valid_at, revision_no)
JOIN selected_run AS r ON r.release_id = i.release_id
GROUP BY r.initialized_at, i.valid_at
ORDER BY i.valid_at;
```

### Case 6. Berlin ensemble temperature forecast

```bash
weather-warehouse plan ensemble
weather-warehouse work
weather-warehouse refresh ensemble
```

An hour with fewer than 20 usable members retains its coverage counts and returns `NULL` percentiles.

```sql
WITH latest AS (
    SELECT r.release_id
    FROM warehouse.gold.forecast_release AS r
    JOIN warehouse.ops.gold_build AS b USING (build_id)
    WHERE b.contract_name = 'ensemble'
      AND r.contract_version = 'v2'
    ORDER BY r.window_start DESC, r.published_at DESC, r.release_id DESC
    LIMIT 1
)
SELECT
    e.valid_at,
    e.p10 AS p10_c,
    e.p50 AS p50_c,
    e.p90 AS p90_c,
    e.expected_members,
    e.usable_members,
    e.coverage_status
FROM warehouse.gold.ensemble_hourly AS e
JOIN latest USING (release_id)
ORDER BY e.valid_at;
```

### Case 8. Berlin daily PM2.5

This example fetches tomorrow’s air-quality forecast for a full Berlin calendar day. Converting both local midnights to UTC preserves the day’s boundaries across daylight saving changes. [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api)

```bash
read -r AQ_DAY AQ_NEXT AQ_START AQ_END < <(python - <<'PY'
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

berlin = ZoneInfo("Europe/Berlin")
day = datetime.now(berlin).date() + timedelta(days=1)
next_day = day + timedelta(days=1)
start = datetime.combine(day, time.min, berlin).astimezone(UTC)
end = datetime.combine(next_day, time.min, berlin).astimezone(UTC)
print(day, next_day, start.isoformat(), end.isoformat())
PY
)

weather-warehouse plan air_quality --start "$AQ_START" --end "$AQ_END"
weather-warehouse work
weather-warehouse publish environment_daily --start "$AQ_DAY" --end "$AQ_NEXT"
```

```sql
SELECT
    d.local_date,
    d.value_number AS mean_pm2_5,
    d.unit,
    d.expected_slots,
    d.usable_slots,
    d.coverage_status
FROM warehouse.gold.environment_daily AS d
JOIN warehouse.ops.gold_build AS b USING (build_id)
WHERE b.contract_name = 'environment_daily'
  AND d.contract_version = 'v2'
  AND d.metric_code = 'pm2_5'
ORDER BY d.local_date;
```

### Case 9. Berlin historical forecast temperature validation

Compare the archived DWD ICON-D2 run from **2026-11-10 00:00 UTC** with ERA5 temperatures for lead hours 1–24. The Single Runs API retrieves the selected model initialization, and the Historical Weather API supplies the ERA5 reanalysis reference. [Single Runs API](https://open-meteo.com/en/docs/single-runs-api), [ERA5 reference data](https://open-meteo.com/en/docs/historical-weather-api)

Complete Case 5 for that run, then stop the UI and download its matching Berlin reference hours. The end timestamp is exclusive. `validation_reference` requests Berlin temperature from ERA5 using the UTC hourly profile.

```bash
weather-warehouse plan validation_reference --start 2026-11-10T01:00:00Z --end 2026-11-11T01:00:00Z
weather-warehouse work && weather-warehouse status
```

The Gold validation view joins the exact forecast revisions pinned to the latest published release of each initialization with the latest stored ERA5 temperature revisions for the same site, profile and hour. It records the forecast publication time, reference load time and reference revision identifiers. Use these results for retrospective model comparison against ERA5 reanalysis.

Restart the UI and run the hourly comparison. For another historical run, use its initialization timestamp in both SQL filters and fetch reference hours covering its Case 5 window.

```sql
SELECT
    initialized_at,
    valid_at,
    lead_hours,
    round(forecast_temp_c, 2) AS forecast_temp_c,
    round(reference_temp_c, 2) AS era5_temp_c,
    round(error_c, 2) AS error_c,
    reference_status,
    forecast_published_at,
    reference_loaded_at,
    reference_revision_no
FROM warehouse.gold.forecast_temperature_validation
WHERE location_key = 'berlin'
  AND initialized_at = TIMESTAMPTZ '2026-11-10 00:00:00+00'
ORDER BY valid_at;
```

`error_c` is forecast temperature minus ERA5 temperature. Missing, unusable or ambiguous references retain their forecast rows and return `NULL` errors. A complete run has 24 matched hours. The summary reports coverage alongside mean absolute error (MAE), root mean squared error (RMSE), and average signed error (bias), all in °C.

```sql
SELECT
    count(*) AS forecast_hours,
    count(error_c) AS matched_hours,
    round(avg(absolute_error_c), 3) AS mae_c,
    round(sqrt(avg(error_c * error_c)), 3) AS rmse_c,
    round(avg(error_c), 3) AS bias_c
FROM warehouse.gold.forecast_temperature_validation
WHERE location_key = 'berlin'
  AND initialized_at = TIMESTAMPTZ '2026-11-10 00:00:00+00';
```

Positive bias means the forecast is warmer on average than ERA5. Negative bias means it is cooler.

## Run a warehouse

After the [browser setup](#analyze-with-the-duckdb-browser-ui), you can use these smaller extraction windows for a quick pipeline run. Stop the browser session first.

```sh
source .venv/bin/activate
weather-warehouse plan forecast
weather-warehouse plan ensemble
weather-warehouse plan archive --start 2025-12-24T23:00Z --end 2026-01-01T01:00Z
weather-warehouse plan archive_daily --start 2025-12-25 --end 2026-01-01
weather-warehouse work
weather-warehouse refresh forecast ensemble weather_daily
weather-warehouse status
```

Install the `analysis` extra for DuckDB's optional Python integrations: `fsspec`, `numpy`, `polars`, and `pyarrow` (which provides `pyarrow.lib`). This also installs the four missing imports shown by Pylance in DuckDB's type stubs.

```sh
.venv/bin/python -m pip install -e '.[analysis]'
```

In VS Code, select `.venv/bin/python` with **Python: Select Interpreter**. If the missing-import warnings persist after installation, reload the VS Code window.

The database defaults to `weather.duckdb` in the current directory. Use `weather-warehouse --db data/test.duckdb …` for an independent database. `plan-group live`, `plan-group reference`, and `plan-group backfill` queue the configured jobs. The backfill group queues the full reference history as well as the product-specific backfills. `work` uses public, non-commercial endpoints. It reserves estimated call units, prioritizes live slices, and records quota deferrals. `--max-requests`, `--max-attempts`, and `--workers` bound a worker invocation. DuckDB keeps a single writer while HTTP requests are concurrent. [Open-Meteo limits and attribution](https://open-meteo.com/en/pricing)

`work` automatically retries transient failures, respecting exponential backoff and `Retry-After`. It waits for at most 60 seconds of backoff per invocation. Longer deferrals remain queued for a later invocation. `--max-requests` counts every HTTP attempt, and `--max-attempts` limits recorded attempts per slice across invocations. Use `--no-wait` for one pass through eligible work.

The default `--http-backend auto` starts with HTTPX and uses installed `curl` on a subsequent attempt after a connection or read failure. Each attempt retains its own Bronze response, timestamps, quota reservation, and validation. Use `--http-backend httpx` or `--http-backend curl` to select one backend explicitly. Connection and read timeouts default to 25 and 60 seconds and accept `--connect-timeout` and `--read-timeout` overrides. Failure output includes the saved task state, attempt count, HTTP status, error details, and next eligible retry time.

Replanning an identical window returns its existing slice. To recover a transient failure that exhausted three attempts, stop the UI and raise the lifetime attempt limit explicitly. Existing attempts remain available for inspection.

```sh
weather-warehouse work --retry-failed --max-attempts 6 && weather-warehouse refresh forecast
```

`--retry-failed` selects failed downloads with transient HTTP or transport errors. A failed publication returns a nonzero exit code. Using `&&` in the shell runs publication after a successful worker invocation. These commands require network access in the terminal or execution environment running them. After they finish, restart the UI startup block to inspect the results.

Use `poll single_runs` or `poll forecast` in place of manually planning those jobs when you want to trigger them from the model update feed. Each poll is stored, and newly triggered slices wait until ten minutes after the feed’s availability timestamp. You can also plan an exact run with `plan single_runs --run TIMESTAMP`. The Single Runs endpoint returns the full run. Silver loads the planned part and retains the whole response in Bronze. [Open-Meteo Model Updates](https://open-meteo.com/en/docs/model-updates)

`refresh` updates configured reporting outputs from the available slices and revised fact intervals. It retains the most recent published forecast when the new window fails the release contract. To build a particular range, pass the start and end dates explicitly.

```sh
weather-warehouse publish weather_daily --start 2025-07-21 --end 2025-07-28
```

Reporting ranges use local dates with an exclusive end. Forecast ranges use offset-qualified, UTC hour-aligned timestamps. The default forecast release requires temperature, precipitation, and wind for 24 consecutive hours from one vintage. The weather report prefers ERA5 daily minimum and maximum temperatures when the source's fixed-offset interval exactly matches the reporting day. It falls back to the pinned ERA5 hourly series if it needs another interval, including 23- and 25-hour reporting days. `metric_coverage` identifies the selected input. Incomplete metrics remain SQL `NULL`.

Subdaily extraction windows must align with their configured cadence: full UTC hours for hourly jobs and 15-minute boundaries for the starter minutely job. Invalid windows are rejected before any slices are queued.

## Inspect Bronze data

Use the full `database.schema.table` name in query cells. This avoids relying on the current database selection carrying over between cells. For example, inspect Berlin's historical Bronze blocks:

```sql
SELECT
    location_key,
    block_code,
    sample_count,
    returned_fields
FROM warehouse.bronze.historical_weather_inspection
WHERE location_key = 'berlin'
  AND block_code IN ('hourly', 'daily')
ORDER BY block_code;
```

After successful archive downloads for the example windows in **Run a warehouse**, one fetch attempt per job produces:

| location_key | block_code | sample_count | returned_fields |
| --- | --- | ---: | --- |
| berlin | daily | 7 | temperature_2m_min, temperature_2m_max |
| berlin | hourly | 170 | precipitation, temperature_2m, wind_gusts_10m |

Bronze retains each fetch attempt, so additional attempts can produce additional rows. The inspection view exposes the raw JSON blocks and their units. To expand the hourly arrays into rows, run:

```sql
WITH raw_hours AS (
    SELECT
        unnest(json_extract(block_json, '$.time[*]'))::BIGINT AS epoch_seconds,
        unnest(json_extract(block_json, '$.temperature_2m[*]'))::DOUBLE AS temp_c,
        unnest(json_extract(block_json, '$.precipitation[*]'))::DOUBLE AS precip_mm,
        unnest(json_extract(block_json, '$.wind_gusts_10m[*]'))::DOUBLE AS gust_kmh
    FROM warehouse.bronze.historical_weather_inspection
    WHERE location_key = 'berlin'
      AND block_code = 'hourly'
)
SELECT
    to_timestamp(epoch_seconds) AS time_utc,
    temp_c,
    precip_mm,
    gust_kmh
FROM raw_hours
ORDER BY epoch_seconds
LIMIT 10;
```

This example uses the starter inventory's Unix timestamps and raw source units: temperature in °C, precipitation in mm, and wind gusts in km/h. Remove `LIMIT 10` to query all samples. For forecast responses, use `warehouse.bronze.weather_forecast_inspection` and the fields returned by that product.

If a query cannot find `warehouse`, restart the UI with the startup block. To verify the attached database paths, run the following in a notebook cell.

```sql
SELECT database_name, path FROM duckdb_databases();
```

## Query and replay

```sql
SELECT * FROM warehouse.gold.forecast_latest_releases;
SELECT * FROM warehouse.gold.forecast_hourly_latest ORDER BY valid_at;
SELECT * FROM warehouse.gold.weather_daily ORDER BY local_date;
SELECT * FROM warehouse.silver.coverage WHERE coverage_status <> 'complete';
SELECT * FROM warehouse.ops.quality_issue;
SELECT api_product, requested_model_selector, block_code, api_field, observed_at, locations
FROM warehouse.ops.verified_capability ORDER BY api_product, block_code, api_field;
```

`gold.series_values` exposes all validated revisions and their requested site, returned grid coordinates, measurement, profile, vintage, and both availability clocks. `gold.release_input` and `ops.gold_build_input` retain the exact Silver facts used for published builds. Bronze inspection views include the raw blocks and units located by JSON paths.

Use `ingest SLICE_ID response.json --requested-at TIMESTAMP --fetched-at TIMESTAMP` to import saved response bytes. Supply `--sha256` to check the bytes against the capture's checksum. `replay SLICE_ID FETCH_ID` revalidates an existing Bronze attempt offline. To correct a mapping, add a higher mapping version in a compatible configured job and replay with `--mapping-job JOB`. Give a changed measurement meaning its own measure definition version.

Build a frozen dataset with `publish training --dataset ID --start UTC_HOUR --end UTC_HOUR --decision TIMESTAMP --cutoff TIMESTAMP`. Pass `--decision` once per decision time. Use `--surface gold` to select the release that was serving at each decision. The default Silver surface uses warehouse timestamps. `--basis verified_source` uses revision-specific evidence stored with `ingest --source-available-at TIMESTAMP --source-reference REFERENCE`. Use `--ordered-correction` with that evidence to append an ordered provider correction. Dataset IDs are immutable.

The v2 training contract includes trailing three-hour precipitation and mean temperature ending at the target hour. Every contributor must pass the decision cutoff and belong to the same selected vintage or served release. `rolling_features` contains the values, and `missing_features` marks unavailable predictors, including optional ones. Use a fresh dataset ID for v2 training. Existing v1 rows remain available from the same query tables.

### Query from Terminal

Use this wrapper to run DuckDB SQL from zsh. Replace the example `SELECT` with any query from this guide. The text between the triple quotes is passed to DuckDB as SQL.

```bash
.venv/bin/python - <<'PY'
import duckdb

with duckdb.connect() as db:
    db.execute("ATTACH 'weather.duckdb' AS warehouse (READ_ONLY)")
    db.execute("SET TimeZone = 'UTC'")
    db.sql("""
        SELECT local_date, temp_min_c, temp_max_c
        FROM warehouse.gold.weather_daily
        ORDER BY local_date DESC
        LIMIT 5;
    """).show()
PY
```

If you open `weather.duckdb` directly in a DuckDB SQL session, omit the `warehouse.` prefix from the table names.

## Configure the inventory

Copy the [TOML file](src/meteo_warehouse/starter.toml) and pass it as `weather-warehouse --config inventory.toml …`. Product entries route requests to their Bronze categories and define the vintage type. Sites supply stable location keys and coordinates. Fields specify canonical units, levels, time windows, types, and member identities when verified. Jobs pick the site batch, selector, request profile, and dates.

Use optional field capability `versions` with half-open `effective_from` and `effective_to` dates. The planner selects the version active at the planning time and pins it to the slice. It applies the strictest applicable `max_locations`, `max_fields`, and `max_days` limits. A capability horizon can use an exact `run`, the original `requested_window` start, or a specific `trigger_run` from the model feed. A trigger-run horizon limits coverage expectations and leaves a live capture's vintage identity separate. Successful field coverage records its observation time and raw fetch ID in `ops.capability_evidence`. Change the Gold contract version when you change its output or selection rules.

Gold contracts also pin an implementation fingerprint based on Python syntax. Comments, formatting outside string literals, the reviewed unused datetime imports, and constant f-string cleanup preserve the fingerprint. SQL text and executable logic remain part of the check. A narrow compatibility bridge accepts the original implementation's stored file hash only for the reviewed equivalent implementation; existing stored hashes and published data are retained. Future implementation changes require a new contract version, including changes to SQL string contents. Do not update the legacy compatibility constant to bypass that check.

### Forecast another city

Forecast jobs accept configured latitude and longitude pairs, including multiple sites in one job. The starter `forecast` job targets Berlin and selects `dwd_icon_d2_native`, a model covering Central Europe. For cities elsewhere, choose a model covering the requested coordinates. Open-Meteo's `best_match` selection uses global models with regional models where available. Available fields and forecast horizons depend on the selected models. [Open-Meteo model selection](https://open-meteo.com/en/docs#best-match-model-selection), [DWD model coverage](https://open-meteo.com/en/docs/dwd-api)

For a 24-hour Sydney forecast, copy the starter inventory and append the tables below to the copy. The example defines a product for automatic model selection, a site, an extraction job, and a Gold release contract.

```sh
cp src/meteo_warehouse/starter.toml inventory.toml
```

```toml
[products.global_forecast]
category = "weather_forecast"
url = "https://api.open-meteo.com/v1/forecast"
vintage = "capture"

[sites.sydney]
latitude = -33.8688
longitude = 151.2093
display_name = "Sydney"

[jobs.sydney_forecast]
product = "global_forecast"
mode = "live"
selector = "best_match"
selector_kind = "blend"
sites = ["sydney"]
block = "hourly"
hours = 24
request_profile = "utc_hourly"
fields = ["temperature_2m", "precipitation", "wind_speed_10m"]

[contracts.sydney_forecast]
kind = "forecast"
version = "v1"
job = "sydney_forecast"
site = "sydney"
hours = 24
required = ["temp_2m_c", "precip_mm", "wind_10m_ms"]
metrics.temp_2m_c = { measure = "temperature_2m", unit = "°C" }
metrics.precip_mm = { measure = "precipitation", unit = "mm", before = 3600 }
metrics.wind_10m_ms = { measure = "wind_speed_10m", unit = "m/s" }
```

Run the new job and publish its forecast in a separate warehouse.

```sh
weather-warehouse --db data/sydney.duckdb --config inventory.toml plan sydney_forecast
weather-warehouse --db data/sydney.duckdb --config inventory.toml work --max-requests 1
weather-warehouse --db data/sydney.duckdb --config inventory.toml refresh sydney_forecast
```

Hourly extraction uses UTC. For another city, give it a stable site key and its own coordinates, then reference that site in the job and Gold contract. A job can fetch several configured sites, and each city needs a corresponding contract to publish its Gold forecast. Add a job to a configured `job_groups` list to include it in `plan-group`. Reporting calendars and benchmark geography are configured separately through reporting contracts and `warehouse.geography`.

## Snapshots and benchmarks

```sh
weather-warehouse snapshot --output data/snapshots
weather-warehouse benchmark --trials 20 --dataset ID
```

`snapshot` writes a checkpointed, content-addressed DuckDB file and atomically updates `latest.json`. Independent readers can open that file read-only while the pipeline writes to the primary database. Schedule snapshot publication between writer invocations. Old snapshot versions remain available to readers pinning their file. Pass a snapshot path with `--db` to benchmark it.

`benchmark` reports live and backfill rates from recorded worker batches and p95 latency for a date-filtered daily report, a multi-year aggregation, and a training export to temporary Parquet. It separates receipt-to-Gold freshness, verified per-input source-to-Gold freshness, and the exact-run model feed clock. Missing observations produce empty metrics. Benchmark windows and trial count are in the inventory.

The local baseline check completed 18 live extraction jobs across the 11 Bronze categories. A separate synthetic three-year backfill loaded 243,330 facts through 158 batches at 364 facts per second. Across 20 trials on its read-only snapshot, p95 was 1.4 ms for the seven-day report, 29.0 ms for the multi-year aggregation, and 1.9 ms for the 352-row Parquet training export.

## Tests and quality checks

Install the development tools and run the quality checks from the project directory. Tool versions, lint rules, formatting, and coverage settings are recorded in `pyproject.toml`.

```sh
python -m pip install -e '.[analysis,dev]'
python -m ruff check src tests
python -m ruff format --check src tests
python -m vulture
python -m coverage erase
python -m coverage run -m unittest discover -s tests -v
python -m coverage combine
python -m coverage report
python -m coverage json -o data/quality/coverage/coverage.json
```

Coverage includes subprocesses used to test both `python -m meteo_warehouse` and the `weather-warehouse` command. The JSON report is saved under the ignored `data/quality/coverage/` directory. You can also run the test suite directly with `python -m unittest discover -s tests -v`.

The suite currently contains 53 tests using deterministic responses, mock HTTP transport, and in-memory or temporary DuckDB databases. They cover CLI entry points and exit codes, checksums and array lengths, zero versus source null, replay and revisions, quotas and model polls, capability dates and limits, release coherence, ensemble gaps, provider fallback on DST days, rolling feature cutoffs, and read-only snapshots.

The latest 11 regression tests also check retry timing for HTTP 429/503 responses and timeouts, numeric and HTTP-date `Retry-After` headers, retry limits, and recovery after a response has landed and processing is interrupted. Recovery reopens a temporary warehouse and verifies that the saved attempt is reused without another HTTP request or duplicate facts. Geocoding and elevation tests check valid, empty, malformed, and partially usable results, location matching, and replay. Training export tests publish frozen datasets, run the benchmark's Parquet export, read the file back, and check row selection, values, timestamps, rolling features, and missing-feature indicators.

Data originates from Open-Meteo and its upstream providers. This pipeline validates, normalizes, and aggregates the responses. [Open-Meteo data sources](https://open-meteo.com/en/features)
