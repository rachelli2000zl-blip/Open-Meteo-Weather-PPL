# Berlin weather pipeline

This local ELT pipeline uses Open-Meteo to show the warmest forecast hour for a synthetic Berlin facility, the spread across ensemble members, and seven days of ERA5 historical context.

The MVP fetches three requests. ICON-D2 supplies 24 hourly forecasts for 2 m temperature, ICON-D2-EPS supplies the same hours for 20 ensemble members, and ERA5 supplies daily minimum and maximum temperatures for 21–27 July 2025. The scope and model IDs are in [the Berlin profile](src/weather_pipeline/berlin.toml). The broader Berlin catalogue is in [the system design](SYSTEM_DESIGN.md).

## Run locally

Use Python 3.12 or newer and `uv` on macOS or Linux. The first command installs the locked dependencies. Replay runs offline after installation.

```bash
uv sync --locked
uv run --locked --offline weather-pipeline replay
```

Open the `outlook.html` path printed by the command. The chart is embedded in the HTML. The bundled fixture is a real Open-Meteo capture from 15 November 2026 UTC, with original bodies, request parameters, and checksums. Replaying it twice reuses the same build ID. A replayed report displays **Recorded snapshot** and the original forecast window.

```bash
uv run --locked weather-pipeline capture
uv run --locked weather-pipeline capture --resume CAPTURE_ID
uv run --locked --offline weather-pipeline replay --capture-dir data/live/raw/CAPTURE_ID
uv run --locked --offline python -m unittest discover -s tests -v
```

The live command requires internet access. It starts a new capture on each run with a 60-second collection budget and up to three attempts per request. Resume keeps the original window and reuses the committed responses. After the budget expires, resume can still build from the responses already on disk.

## Outputs and quality rules

| Layer | Output |
| --- | --- |
| Bronze | `data/live/raw/<capture_id>/capture.json` and per-attempt response bytes and metadata. The body checksum covers the exact bytes passed to the JSON parser after HTTP decompression. |
| Silver | `weather_samples.parquet`, `weather_daily.parquet`, and `coverage.parquet`. Rows retain source field, unit, and request lineage. Coverage is tracked per forecast field or ensemble series. |
| Gold | `outlook.html`, `outlook_hourly.csv`, `coverage.csv`, and `quality_issues.csv`. Live and replay builds have separate output directories. Each directory’s `latest.json` points to its latest successful report. |

A **passed** or **partial** run publishes and exits `0`. Partial coverage includes optional ERA5 failures and hours with an incomplete ensemble roster. The chart shows the P10–P90 band only for hours with all 20 valid members. A **blocked** run exits `2` when any target hour lacks usable deterministic temperature or all recognized ensemble members. It writes diagnostics and leaves the latest report pointer in place. Local execution errors exit `1`. A single writer lock and atomic publication keep unfinished builds from becoming visible as the latest report.

Bronze retains malformed and HTTP error bodies. Silver verifies exact time slots, array alignment, types, units, and checksums. It preserves source nulls, records missing values separately, collapses identical duplicates, and quarantines conflicting values. The fixture yields 504 hourly records and 14 daily records.

## Data attribution

The bundled responses are from Open-Meteo and include DWD weather forecasts and ERA5 reanalysis. The pipeline validates and reshapes the responses and calculates the ensemble P10–P90 band. The bundled response bodies are unchanged. [Open-Meteo](https://open-meteo.com/), [Open-Meteo data license and attribution](https://open-meteo.com/en/pricing#attribution)
