from datetime import UTC, datetime
from html import escape
from importlib.resources import files
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import duckdb
import plotly.graph_objects as go

from .bronze import CONFIG, PROFILE, atomic_json, json_bytes, read_json, sha256
from .silver import Quality, capture_slots, read_capture, read_responses, transform


COMMON_FIELDS = (
    "capture_id VARCHAR, site_id VARCHAR, product VARCHAR, model_id VARCHAR, section VARCHAR"
)
VALUE_FIELDS = (
    "variable_id VARCHAR, series_key VARCHAR, source_field VARCHAR, unit VARCHAR, "
    "value_number DOUBLE, request_id VARCHAR, raw_path VARCHAR"
)
SAMPLE_SCHEMA = f"""
    {COMMON_FIELDS}, valid_at_utc TIMESTAMPTZ, {VALUE_FIELDS},
    UNIQUE (capture_id, product, site_id, model_id, section, valid_at_utc, variable_id, series_key)
"""
DAILY_SCHEMA = f"""
    {COMMON_FIELDS}, local_date DATE, timezone VARCHAR, {VALUE_FIELDS},
    UNIQUE (capture_id, product, site_id, model_id, section, local_date, timezone, variable_id, series_key)
"""
COVERAGE_SCHEMA = """
    request_id VARCHAR, product VARCHAR, model_id VARCHAR, section VARCHAR, variable_id VARCHAR,
    series_key VARCHAR, source_field VARCHAR, expected_slots INTEGER, returned_slots INTEGER,
    valid_values INTEGER, source_nulls INTEGER, invalid_values INTEGER, missing_slots INTEGER,
    duplicate_values INTEGER, status VARCHAR
"""
ISSUE_SCHEMA = """
    request_id VARCHAR, product VARCHAR, section VARCHAR, raw_path VARCHAR,
    code VARCHAR, detail VARCHAR, source_field VARCHAR, slot VARCHAR
"""


def stage_table(conn, name, schema, rows):
    conn.execute(f"CREATE TABLE {name} ({schema})")
    if rows:
        params = ",".join("?" for _ in rows[0])
        conn.executemany(f"INSERT INTO {name} VALUES ({params})", rows)


def write_tables(stage, quality, hours, dates):
    count = CONFIG["ensemble"]["members"]
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MB"}) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        stage_table(conn, "weather_samples", SAMPLE_SCHEMA, quality.samples)
        stage_table(conn, "weather_daily", DAILY_SCHEMA, quality.daily)
        stage_table(conn, "coverage", COVERAGE_SCHEMA, quality.coverage)
        stage_table(conn, "quality_issues", ISSUE_SCHEMA, quality.issues)
        stage_table(conn, "forecast_hours", "valid_at_utc TIMESTAMPTZ",
                    [(datetime.fromtimestamp(hour, UTC),) for hour in hours])
        stage_table(conn, "archive_dates", "local_date DATE", [(day,) for day in dates])
        conn.execute("""
            CREATE TABLE outlook_hourly AS
            WITH ensemble AS (
                SELECT valid_at_utc, count(value_number)::INTEGER AS valid_members,
                       CASE WHEN count(value_number) = ? THEN quantile_cont(value_number, 0.10) END AS p10,
                       CASE WHEN count(value_number) = ? THEN quantile_cont(value_number, 0.90) END AS p90
                FROM weather_samples WHERE product = 'ensemble' AND variable_id = 'temperature_2m'
                GROUP BY valid_at_utc
            )
            SELECT h.valid_at_utc, f.value_number AS forecast_c, e.p10, e.p90,
                   coalesce(e.valid_members, 0) AS valid_members, ?::INTEGER AS expected_members,
                   f.request_id, f.raw_path
            FROM forecast_hours h
            LEFT JOIN weather_samples f ON f.valid_at_utc = h.valid_at_utc
                AND f.product = 'forecast' AND f.variable_id = 'temperature_2m'
            LEFT JOIN ensemble e ON e.valid_at_utc = h.valid_at_utc
            ORDER BY h.valid_at_utc
        """, [count, count, count])
        daily = conn.execute("""
            SELECT d.local_date,
                   min(w.value_number) FILTER (WHERE w.variable_id = 'temperature_2m_min'),
                   max(w.value_number) FILTER (WHERE w.variable_id = 'temperature_2m_max'),
                   any_value(w.request_id), any_value(w.raw_path)
            FROM archive_dates d LEFT JOIN weather_daily w USING (local_date)
            GROUP BY d.local_date ORDER BY d.local_date
        """).fetchall()
        issues = []
        for day, low, high, request_id, raw_path in daily:
            if low is not None and high is not None and low > high:
                issues.append((request_id, "archive", "daily", raw_path, "invalid_daily_range",
                               "Daily minimum exceeds maximum", None, day.isoformat()))
        if issues:
            conn.executemany("INSERT INTO quality_issues VALUES (?, ?, ?, ?, ?, ?, ?, ?)", issues)
        for name in ("weather_samples", "weather_daily", "coverage"):
            conn.execute(f"COPY {name} TO ? (FORMAT PARQUET)", [str(stage / f"{name}.parquet")])
        for name in ("coverage", "quality_issues", "outlook_hourly"):
            conn.execute(f"COPY {name} TO ? (FORMAT CSV, HEADER TRUE)", [str(stage / f"{name}.csv")])
        hourly = conn.execute("""
            SELECT epoch(valid_at_utc)::BIGINT, * EXCLUDE (valid_at_utc)
            FROM outlook_hourly ORDER BY valid_at_utc
        """).fetchall()
        hourly = [(datetime.fromtimestamp(slot, UTC), *values) for slot, *values in hourly]
        counts = {name: conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                  for name in ("weather_samples", "weather_daily", "coverage", "quality_issues")}
    return hourly, daily, counts


def temperature(value):
    return "Unavailable" if value is None else f"{value:.1f} °C"


def render_report(capture, status, hourly, daily, recorded):
    tz = ZoneInfo(capture["timezone"])
    labels = [row[0].astimezone(tz).strftime("%d %b %H:%M %Z") for row in hourly]
    peak = max(range(len(hourly)), key=lambda index: hourly[index][1])
    _, value, low, high, actual, expected, request_id, raw_path = hourly[peak]
    spread = f"{low:.1f}–{high:.1f} °C" if low is not None else f"Unavailable ({actual}/{expected} members)"
    period = [datetime.fromisoformat(capture["window"][key]).astimezone(tz).strftime("%d %b %Y %H:%M %Z")
              for key in ("forecast_start_utc", "forecast_stop_utc")]

    fig = go.Figure()
    first_band = True
    for complete, group in groupby(range(len(hourly)), key=lambda index: hourly[index][2] is not None):
        if not complete:
            continue
        points = list(group)
        x = [labels[index] for index in points]
        lower = [hourly[index][2] for index in points]
        upper = [hourly[index][3] for index in points]
        fig.add_scatter(x=x + x[::-1], y=upper + lower[::-1], mode="lines", fill="toself",
                        name="Ensemble P10–P90", legendgroup="ensemble", showlegend=first_band,
                        line={"color": "rgba(28,116,119,.35)", "width": 1},
                        fillcolor="rgba(28,116,119,.20)", hoverinfo="skip")
        first_band = False
    fig.add_scatter(x=labels, y=[row[1] for row in hourly], mode="lines+markers",
                    name="ICON-D2 forecast", line={"color": "#145459", "width": 3},
                    customdata=[[row[4], row[5]] for row in hourly],
                    hovertemplate="%{x}<br>%{y:.1f} °C<br>%{customdata[0]}/%{customdata[1]} members<extra></extra>")
    fig.update_layout(template="plotly_white", height=410, margin={"l": 52, "r": 24, "t": 26, "b": 96},
                      yaxis_title="Temperature (°C)", xaxis={"type": "category", "tickangle": -35,
                      "categoryorder": "array", "categoryarray": labels, "tickmode": "array",
                      "tickvals": labels[::3]}, legend={"orientation": "h"})
    chart = fig.to_html(full_html=False, include_plotlyjs=True, div_id="hourly-forecast",
                        config={"responsive": True, "displayModeBar": False})
    rows = []
    for day, minimum, maximum, _, _ in daily:
        if minimum is not None and maximum is not None and minimum > maximum:
            minimum = maximum = None
        rows.append(f"<tr><th scope=row>{day}</th><td>{temperature(minimum)}</td><td>{temperature(maximum)}</td></tr>")
    history = "".join(rows)
    freshness = "Recorded snapshot" if recorded else "Live capture"
    stop = escape(capture["window"]["forecast_stop_utc"], quote=True)
    start = escape(capture["window"]["forecast_start_utc"], quote=True)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Berlin facility weather outlook</title>
<style>
body{{font:16px/1.55 system-ui,sans-serif;background:#f0f5f5;color:#172526;margin:0;padding:1.5rem}}
main{{max-width:960px;margin:auto;background:white;padding:clamp(1rem,4vw,2.5rem);border:1px solid #cbdad9;border-radius:1rem}}
h1,h2{{line-height:1.15}} .peak{{background:#e4f1ef;padding:1rem;border-radius:.6rem}}
table{{width:100%;border-collapse:collapse;text-align:left}} th,td{{border-bottom:1px solid #d7e1e0;padding:.5rem}}
a{{color:#145459}} code{{overflow-wrap:anywhere}} #hourly-forecast{{min-width:0}}
</style></head><body><main>
<h1>Berlin facility weather outlook</h1>
<p><strong id="freshness" data-start="{start}" data-stop="{stop}" data-recorded="{str(recorded).lower()}">{freshness}</strong> · Coverage {status}<br>
Forecast window {period[0]} to {period[1]} (end exclusive)<br>
Capture {escape(capture['captured_at_utc'])}</p>
<section class="peak"><h2>Warmest forecast hour</h2>
<p><strong>{labels[peak]} · {temperature(value)}</strong><br>Ensemble P10–P90 {spread}</p></section>
<h2>Hourly forecast and ensemble spread</h2>
{chart}
<p>Read the <a href="outlook_hourly.csv">hourly values and member counts</a>. Gaps indicate an incomplete ensemble roster.</p>
<h2>Historical reanalysis · ERA5</h2>
<table><thead><tr><th scope=col>Berlin date</th><th scope=col>Minimum</th><th scope=col>Maximum</th></tr></thead>
<tbody>{history}</tbody></table>
<h2>Coverage and source</h2>
<p><a href="coverage.csv">Field coverage</a> · <a href="quality_issues.csv">Quality issues</a></p>
<p>Peak trace <code>{escape(capture['capture_id'])} / {escape(request_id)} / {escape(raw_path)}</code> · <code>hourly.temperature_2m</code></p>
<p>Data from <a href="https://open-meteo.com/">Open-Meteo</a>. Forecasts from DWD ICON-D2 and ICON-D2-EPS. Historical context from ERA5.
The forecast products are fetched independently. The reported band summarizes the available model scenarios.</p>
</main><script>
const badge = document.getElementById('freshness');
if (badge.dataset.recorded === 'false') {{
  if (Date.now() >= Date.parse(badge.dataset.stop)) badge.textContent = 'Forecast window expired';
  else if (Date.now() > Date.parse(badge.dataset.start)) badge.textContent = 'Forecast window already started';
}}
</script></body></html>"""


def build(capture_dir, root, recorded):
    capture = read_capture(capture_dir)
    hours, dates = capture_slots(capture)
    quality = Quality()
    responses, fingerprint = read_responses(capture_dir, capture, quality)
    transform(capture, responses, hours, dates, quality)

    package = files("weather_pipeline")
    version = [sha256(package.joinpath(name).read_bytes()) for name in ("bronze.py", "silver.py", "publish.py")]
    build_id = sha256(json_bytes([capture, fingerprint, sha256(PROFILE), version, recorded]))[:20]
    capture_id = capture["capture_id"]
    parent = root / "builds" / capture_id
    destination = parent / build_id
    parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        summary = read_json(destination / "manifest.json")
        for name, digest in summary["output_checksums"].items():
            if sha256((destination / name).read_bytes()) != digest:
                raise ValueError(f"Existing build is corrupt: {destination / name}")
    else:
        with TemporaryDirectory(dir=parent, prefix=".staging-") as temporary:
            stage = Path(temporary) / "output"
            stage.mkdir()
            hourly, daily, counts = write_tables(stage, quality, hours, dates)
            missing_forecast = [row[0].isoformat() for row in hourly if row[1] is None]
            missing_ensemble = [row[0].isoformat() for row in hourly if row[4] == 0]
            partial = counts["quality_issues"] or any(row[-1] != "populated" for row in quality.coverage)
            status = "blocked" if missing_forecast or missing_ensemble else "partial" if partial else "passed"
            if status != "blocked":
                (stage / "outlook.html").write_text(render_report(capture, status, hourly, daily, recorded),
                                                   encoding="utf-8")
            summary = {"capture_id": capture_id, "build_id": build_id, "status": status, "counts": counts,
                       "window": capture["window"], "missing_forecast_hours": missing_forecast,
                       "empty_ensemble_hours": missing_ensemble, "recorded": recorded,
                       "output_checksums": {p.name: sha256(p.read_bytes()) for p in sorted(stage.iterdir())}}
            (stage / "manifest.json").write_bytes(json_bytes(summary))
            stage.rename(destination)

    if summary["status"] != "blocked":
        atomic_json(root / "latest.json", {"status": summary["status"], "build_id": build_id,
                    "report": (destination / "outlook.html").relative_to(root).as_posix()})
    return summary, destination
