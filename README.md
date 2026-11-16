# Open-Meteo warehouse

The pipeline implements the Bronze, Silver, and Gold layers in [WAREHOUSE_DESIGN.md](WAREHOUSE_DESIGN.md). Python plans and fetches extraction slices. DuckDB stores raw attempts, validated series and revisions, reporting tables, and frozen training datasets.

The [baseline-v2 inventory](src/meteo_warehouse/starter.toml) configures all 11 Bronze categories. It covers Berlin weather, air quality and solar irradiance, marine data off Helgoland, and river discharge in Oslo. ERA5 reference weather covers all three sites for calendar years 2023–2025. Specialized products have their own dates, models and horizons in the inventory. `utc_hourly` and `berlin_calendar` are reusable request profiles. The baseline retains raw evidence, revisions and published manifests. Set your own sites, fields and dates through TOML. [Open-Meteo API catalog](https://open-meteo.com/en/docs)

## Run a warehouse

Requires Python 3.12 or later. Current environment: **Python 3.14.6**.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
weather-warehouse init
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

The database defaults to `weather.duckdb` in the current directory. Use `weather-warehouse --db data/test.duckdb …` for an independent database. `plan-group live`, `plan-group reference`, and `plan-group backfill` queue the configured jobs. The backfill group queues the full reference history as well as the product-specific backfills. `work` uses public, non-commercial endpoints. It reserves estimated call units, prioritizes live slices, and records quota deferrals. Repeat it to process queued work and eligible retries. `--max-requests`, `--max-attempts`, and `--workers` bound a worker invocation. DuckDB keeps a single writer while HTTP requests are concurrent. [Open-Meteo limits and attribution](https://open-meteo.com/en/pricing)

Use `poll single_runs` or `poll forecast` in place of manually planning those jobs when you want to trigger them from the model update feed. Each poll is stored, and newly triggered slices wait until ten minutes after the feed’s availability timestamp. You can also plan an exact run with `plan single_runs --run TIMESTAMP`. The Single Runs endpoint returns the full run. Silver loads the planned part and retains the whole response in Bronze. [Open-Meteo Model Updates](https://open-meteo.com/en/docs/model-updates)

`refresh` updates configured reporting outputs from the available slices and revised fact intervals. It retains the most recent published forecast when the new window fails the release contract. To build a particular range, pass the start and end dates explicitly.

```sh
weather-warehouse publish weather_daily --start 2025-07-21 --end 2025-07-28
```

Reporting ranges use local dates with an exclusive end. Forecast ranges use offset-qualified, UTC hour-aligned timestamps. The default forecast release requires temperature, precipitation, and wind for 24 consecutive hours from one vintage. The weather report prefers ERA5 daily minimum and maximum temperatures when the source's fixed-offset interval exactly matches the reporting day. It falls back to the pinned ERA5 hourly series if it needs another interval, including 23- and 25-hour reporting days. `metric_coverage` identifies the selected input. Incomplete metrics remain SQL `NULL`.

Subdaily extraction windows must align with their configured cadence: full UTC hours for hourly jobs and 15-minute boundaries for the starter minutely job. Invalid windows are rejected before any slices are queued.

## Analyze with the DuckDB browser UI

Use the DuckDB UI to write SQL in notebook cells and view the results as tables. Complete the installation and fetch steps above first. Start the UI from the project directory:

```sh
.venv/bin/python -c '
from pathlib import Path
import duckdb

warehouse = Path("weather.duckdb").resolve(strict=True)
Path("data").mkdir(exist_ok=True)
db = duckdb.connect("data/analysis-ui.duckdb")
print(f"Warehouse: {warehouse}")
db.execute("CALL start_ui()")
input("DuckDB UI is running. Press Enter to stop.\n")
db.close()
'
```

The browser should open automatically. Otherwise, visit [localhost:4213](http://localhost:4213). Keep the terminal session running while using the UI; press Enter in that terminal to stop it. The UI needs internet access to download its extension and frontend. Queries against the local warehouse execute on your computer. [DuckDB UI documentation](https://duckdb.org/docs/1.5/core_extensions/ui)

Create a notebook in the UI, paste the following SQL into a cell, and click **Run**:

```sql
ATTACH IF NOT EXISTS 'weather.duckdb' AS warehouse (READ_ONLY);
SET TimeZone = 'UTC';
```

The relative path uses the project directory where you started the UI. If you use a different directory or database, replace `weather.duckdb` with its absolute path. The startup command prints the absolute path of the default warehouse. The separate `data/analysis-ui.duckdb` file stores UI notebooks, while the warehouse is attached read-only. Finish pipeline writer commands before attaching the primary warehouse, and stop the UI before running another writer command. To analyze data while the pipeline writes, attach a published [snapshot](#snapshots-and-benchmarks) instead.

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

If a query reports that schema `bronze` does not exist, run the attachment cell and use `warehouse.bronze.historical_weather_inspection` rather than `bronze.historical_weather_inspection`. To verify the attached database paths, run:

```sql
SELECT database_name, path FROM duckdb_databases();
```

## Query and replay

In the UI connection above, prefix the table names below with `warehouse.`, for example `warehouse.gold.weather_daily`.

```sql
SELECT * FROM gold.forecast_latest_releases;
SELECT * FROM gold.forecast_hourly_latest ORDER BY valid_at;
SELECT * FROM gold.weather_daily ORDER BY local_date;
SELECT * FROM silver.coverage WHERE coverage_status <> 'complete';
SELECT * FROM ops.quality_issue;
SELECT api_product, requested_model_selector, block_code, api_field, observed_at, locations
FROM ops.verified_capability ORDER BY api_product, block_code, api_field;
```

`gold.series_values` exposes all validated revisions and their requested site, returned grid coordinates, measurement, profile, vintage, and both availability clocks. `gold.release_input` and `ops.gold_build_input` retain the exact Silver facts used for published builds. Bronze inspection views include the raw blocks and units located by JSON paths.

Use `ingest SLICE_ID response.json --requested-at TIMESTAMP --fetched-at TIMESTAMP` to import saved response bytes. Supply `--sha256` to check the bytes against the capture's checksum. `replay SLICE_ID FETCH_ID` revalidates an existing Bronze attempt offline. To correct a mapping, add a higher mapping version in a compatible configured job and replay with `--mapping-job JOB`. Give a changed measurement meaning its own measure definition version.

Build a frozen dataset with `publish training --dataset ID --start UTC_HOUR --end UTC_HOUR --decision TIMESTAMP --cutoff TIMESTAMP`. Pass `--decision` once per decision time. Use `--surface gold` to select the release that was serving at each decision. The default Silver surface uses warehouse timestamps. `--basis verified_source` uses revision-specific evidence stored with `ingest --source-available-at TIMESTAMP --source-reference REFERENCE`. Use `--ordered-correction` with that evidence to append an ordered provider correction. Dataset IDs are immutable.

The v2 training contract includes trailing three-hour precipitation and mean temperature ending at the target hour. Every contributor must pass the decision cutoff and belong to the same selected vintage or served release. `rolling_features` contains the values, and `missing_features` marks unavailable predictors, including optional ones. Use a fresh dataset ID for v2 training. Existing v1 rows remain available from the same query tables.

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

The suite currently contains 46 tests using deterministic responses, mock HTTP transport, and in-memory or temporary DuckDB databases. They cover CLI entry points and exit codes, checksums and array lengths, zero versus source null, replay and revisions, quotas and model polls, capability dates and limits, release coherence, ensemble gaps, provider fallback on DST days, rolling feature cutoffs, and read-only snapshots.

The latest 11 regression tests also check retry timing for HTTP 429/503 responses and timeouts, numeric and HTTP-date `Retry-After` headers, retry limits, and recovery after a response has landed and processing is interrupted. Recovery reopens a temporary warehouse and verifies that the saved attempt is reused without another HTTP request or duplicate facts. Geocoding and elevation tests check valid, empty, malformed, and partially usable results, location matching, and replay. Training export tests publish frozen datasets, run the benchmark's Parquet export, read the file back, and check row selection, values, timestamps, rolling features, and missing-feature indicators.

Data originates from Open-Meteo and its upstream providers. This pipeline validates, normalizes, and aggregates the responses. [Open-Meteo data sources](https://open-meteo.com/en/features)
