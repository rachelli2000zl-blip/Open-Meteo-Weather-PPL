# Open-Meteo warehouse

The pipeline implements the Bronze, Silver, and Gold layers in [WAREHOUSE_DESIGN.md](WAREHOUSE_DESIGN.md). Python plans and fetches extraction slices. DuckDB stores raw attempts, validated series and revisions, reporting tables, and frozen training datasets.

The [baseline-v2 inventory](src/meteo_warehouse/starter.toml) configures all 11 Bronze categories. It covers Berlin weather, air quality and solar irradiance, marine data off Helgoland, and river discharge in Oslo. ERA5 reference weather covers all three sites for calendar years 2023–2025. Specialized products have their own dates, models and horizons in the inventory. `utc_hourly` and `berlin_calendar` are reusable request profiles. The baseline retains raw evidence, revisions and published manifests. Set your own sites, fields and dates through TOML. [Open-Meteo API catalog](https://open-meteo.com/en/docs)

## Run a warehouse

Install with Python 3.12 or later.

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

The database defaults to `weather.duckdb` in the current directory. Use `weather-warehouse --db data/test.duckdb …` for an independent database. `plan-group live`, `plan-group reference`, and `plan-group backfill` queue the configured jobs. The backfill group queues the full reference history as well as the product-specific backfills. `work` uses public, non-commercial endpoints. It reserves estimated call units, prioritizes live slices, and records quota deferrals. Repeat it to process queued work and eligible retries. `--max-requests`, `--max-attempts`, and `--workers` bound a worker invocation. DuckDB keeps a single writer while HTTP requests are concurrent. [Open-Meteo limits and attribution](https://open-meteo.com/en/pricing)

Use `poll single_runs` or `poll forecast` in place of manually planning those jobs when you want to trigger them from the model update feed. Each poll is stored, and newly triggered slices wait until ten minutes after the feed’s availability timestamp. You can also plan an exact run with `plan single_runs --run TIMESTAMP`. The Single Runs endpoint returns the full run. Silver loads the planned part and retains the whole response in Bronze. [Open-Meteo Model Updates](https://open-meteo.com/en/docs/model-updates)

`refresh` updates configured reporting outputs from the available slices and revised fact intervals. It retains the most recent published forecast when the new window fails the release contract. To build a particular range, pass the start and end dates explicitly.

```sh
weather-warehouse publish weather_daily --start 2025-07-21 --end 2025-07-28
```

Reporting ranges use local dates with an exclusive end. Forecast ranges use offset-qualified, UTC hour-aligned timestamps. The default forecast release requires temperature, precipitation, and wind for 24 consecutive hours from one vintage. The weather report prefers ERA5 daily minimum and maximum temperatures when the source's fixed-offset interval exactly matches the reporting day. It falls back to the pinned ERA5 hourly series if it needs another interval, including 23- and 25-hour reporting days. `metric_coverage` identifies the selected input. Incomplete metrics remain SQL `NULL`.

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

## Snapshots and benchmarks

```sh
weather-warehouse snapshot --output data/snapshots
weather-warehouse benchmark --trials 20 --dataset ID
```

`snapshot` writes a checkpointed, content-addressed DuckDB file and atomically updates `latest.json`. Independent readers can open that file read-only while the pipeline writes to the primary database. Schedule snapshot publication between writer invocations. Old snapshot versions remain available to readers pinning their file. Pass a snapshot path with `--db` to benchmark it.

`benchmark` reports live and backfill rates from recorded worker batches and p95 latency for a date-filtered daily report, a multi-year aggregation, and a training export to temporary Parquet. It separates receipt-to-Gold freshness, verified per-input source-to-Gold freshness, and the exact-run model feed clock. Missing observations produce empty metrics. Benchmark windows and trial count are in the inventory.

The local baseline check completed 18 live extraction jobs across the 11 Bronze categories. A separate synthetic three-year backfill loaded 243,330 facts through 158 batches at 364 facts per second. Across 20 trials on its read-only snapshot, p95 was 1.4 ms for the seven-day report, 29.0 ms for the multi-year aggregation, and 1.9 ms for the 352-row Parquet training export.

```sh
python -m unittest discover -s tests -v
```

The tests use deterministic responses and a mock HTTP transport. They cover checksums and array lengths, zero versus source null, replay and revisions, quotas and model polls, capability dates and limits, release coherence, ensemble gaps, provider fallback on DST days, rolling feature cutoffs, and read-only snapshots.

Data originates from Open-Meteo and its upstream providers. This pipeline validates, normalizes, and aggregates the responses. [Open-Meteo data sources](https://open-meteo.com/en/features)
