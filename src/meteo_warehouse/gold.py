import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from .db import PACKAGE, digest, insert, insert_many, instant, json_text, key, now, transaction
from .plan import NO_SELECTOR, local_midnight, profile_data, resolve_job


FORECAST_COLUMNS = ("temp_2m_c", "precip_mm", "wind_10m_ms")
WEATHER_COLUMNS = ("temp_min_c", "temp_max_c", "precip_mm", "wind_gust_max_ms")
FEATURE_COLUMNS = ("forecast_temp_c", "forecast_precip_mm")


def scope_for(config, job_name, site):
    job = resolve_job(config, job_name)
    if site not in job["sites"]:
        raise ValueError(f"Location {site} is outside the job's scope")
    block = job.get("block")
    profile = profile_data(job.get("params", {}) | {"timezone": job.get("timezone", "UTC")},
                           job.get("profile"), calendar=block in {"daily", "weekly", "monthly"})
    return [key(["site", site]), key(["product", job["product"]]),
            job.get("selector", NO_SELECTOR), key(["profile", profile])]


def gold_contract(db, config, name):
    contract = config["contracts"][name]
    scope = scope_for(config, contract["job"], contract["site"])
    pinned = dict(contract, scope=scope)
    reporting = contract["kind"] in {"weather_daily", "environment_daily"}
    for code, metric in contract["metrics"].items():
        if "provider" in metric:
            if not reporting or metric.get("block", "hourly") != "hourly":
                raise ValueError(f"Provider fallback for {code} requires an hourly reporting metric")
            provider = metric["provider"]
            job = resolve_job(config, provider["job"])
            provider_scope = scope_for(config, provider["job"], contract["site"])
            if job.get("block") != "daily" or provider_scope[:3] != scope[:3]:
                raise ValueError(f"Provider daily metric {code} must use the same site, product, and selector")
            pinned.setdefault("provider_scopes", {})[code] = provider_scope
    if contract["kind"] == "ml":
        pinned["label_scope"] = scope_for(config, contract["label_job"], contract["site"])
        release_name = contract.get("release_contract")
        if release_name:
            release = config["contracts"][release_name]
            if release["kind"] != "forecast" or scope != scope_for(config, release["job"], release["site"]):
                raise ValueError("The release contract must publish the feature scope")
            pinned["release_version"] = release["version"]
    sql_hash = hashlib.sha256(PACKAGE.joinpath("gold.py").read_bytes()).hexdigest()
    prior = db.execute("""
        SELECT selection_rules, sql_sha256 FROM ops.gold_contract
        WHERE contract_name = ? AND contract_version = ?
    """, [name, contract["version"]]).fetchone()
    if prior is None:
        insert(db, "ops.gold_contract", [name, contract["version"], json_text(pinned), sql_hash])
    elif json.loads(prior[0]) != json.loads(json_text(pinned)) or prior[1] != sql_hash:
        raise ValueError(f"Bump contract {name}'s version before changing its definition or SQL")
    return pinned


def build_record(db, name, contract, params, output, manifest, cutoff, quality):
    identity = digest([name, contract, params, output, manifest])
    if db.execute("SELECT 1 FROM ops.gold_build WHERE build_id = ?", [identity]).fetchone():
        return identity, False
    insert(db, "ops.gold_build", [
        identity, name, contract["version"], cutoff, "0.2.0", digest(contract),
        json_text([contract["scope"]]), json_text({"rows": len(output)}), json_text(quality), now()])
    insert_many(db, "ops.gold_build_input", [(identity, *row) for row in manifest])
    return identity, True


def manifest_from(db, *tables):
    return db.execute(" UNION ".join(
        f"SELECT DISTINCT series_id, vintage_id, valid_at, revision_no FROM {table} WHERE series_id IS NOT NULL"
        for table in tables) + " ORDER BY 1, 2, 3, 4").fetchall()


def scope_filter(alias="f"):
    return " AND ".join(f"{alias}.{column} = ?" for column in
                        ("site_id", "product_id", "requested_model_selector", "profile_id"))


def scope_join(left, right):
    return " AND ".join(f"{left}.{col} = {right}.{col}" for col in
                        ("site_id", "product_id", "requested_model_selector", "profile_id"))


def metrics_stage(db, contract, columns=None):
    metrics = contract["metrics"]
    if columns and set(metrics) - set(columns):
        raise ValueError("Contract contains a metric outside the physical output columns")
    rolling = contract.get("rolling", {})
    if rolling and (contract["kind"] != "ml" or metrics.keys() & rolling.keys()):
        raise ValueError("Rolling metrics must have unique names in an ML contract")
    required = set(contract.get("required", metrics.keys()))
    metrics = metrics | rolling
    if required - metrics.keys() or not metrics:
        raise ValueError("Required metrics must be defined by the contract")
    result = []
    for code, spec in metrics.items():
        before, after = spec.get("before", 0), spec.get("after", 0)
        reducer = spec.get("reduce", "avg")
        lookback = spec.get("hours", 1)
        if code in rolling and (type(lookback) is not int or lookback < 2 or spec.get("block", "hourly") != "hourly"):
            raise ValueError(f"Rolling input {code} requires an hourly measure and at least two hours")
        if (reducer not in {"min", "max", "sum", "avg"} or before < 0 or after < 0 or (before and after)
                or spec.get("block", "hourly") not in {"daily", "hourly"}):
            raise ValueError(f"Invalid interval or aggregation for {code}")
        if spec.get("members") is not None and (
                not spec["members"] or len(set(spec["members"])) != len(spec["members"])):
            raise ValueError(f"Expected member roster must identify distinct members for {code}")
        if contract["kind"] in {"weather_daily", "environment_daily"} and (
                spec.get("block", "hourly") == "hourly"
                and before + after != (3600 if reducer == "sum" else before + after)
                or spec.get("block", "hourly") == "hourly" and (before not in {0, 3600} or after not in {0, 3600})):
            raise ValueError("Daily sums require full non-overlapping hourly intervals")
        result.append((code, spec["measure"], spec.get("definition_version", 1), spec["unit"],
                       spec.get("block", "hourly"),
                       before, after, reducer, code in required, spec.get("members"),
                       spec.get("allow_flagged", False), lookback if code in rolling else 1))
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_metric (
            metric VARCHAR PRIMARY KEY, measure_code VARCHAR, definition_version INTEGER, unit VARCHAR,
            block_code VARCHAR, seconds_before INTEGER, seconds_after INTEGER, reducer VARCHAR,
            required BOOLEAN, roster INTEGER[], allow_flagged BOOLEAN, lookback_hours INTEGER
        )
    """)
    insert_many(db, "g_metric", result)


def hour_grid(db, start, end):
    start, end = instant(start), instant(end)
    if end <= start or any(value.minute or value.second or value.microsecond for value in (start, end)):
        raise ValueError("Use a half-open UTC hour-aligned window")
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_hour AS
        SELECT range AS valid_at FROM range(?::TIMESTAMPTZ, ?::TIMESTAMPTZ, INTERVAL '1 hour')
    """, [start, end])
    return start, end


def metric_join(f="f", m="m"):
    return f"""
        {f}.measure_code = {m}.measure_code AND {f}.definition_version = {m}.definition_version
        AND {f}.canonical_unit = {m}.unit AND {f}.block_code = {m}.block_code
        AND {f}.scenario_code IS NULL AND {f}.fixed_lead_hours IS NULL
    """


def interval_join(f="f", m="m"):
    return f"""
        {f}.window_start = {f}.valid_at - {m}.seconds_before * INTERVAL '1 second'
        AND {f}.window_end = {f}.valid_at + {m}.seconds_after * INTERVAL '1 second'
    """


def usable(f="f", m="m"):
    return f"{f}.value_number IS NOT NULL AND ({f}.quality_status = 'valid' OR ({m}.allow_flagged AND {f}.quality_status = 'flagged'))"


def pivot(columns, alias=""):
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"max({prefix}value_number) FILTER (WHERE {prefix}metric = '{name}')" for name in columns)


def create_forecast_candidates(db, contract, cutoff):
    member = contract["kind"] == "ensemble"
    valid_members = "(m.roster IS NULL OR list_contains(m.roster, f.member_number))" if member else "true"
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE g_candidates AS
        WITH visible AS (
            SELECT f.*, m.metric, m.required, {usable()} AS usable, {interval_join()} AS aligned
            FROM gold.series_values f JOIN g_hour USING (valid_at)
            JOIN g_metric m ON {metric_join()}
            WHERE {scope_filter()} AND f.warehouse_available_at <= ?
              AND f.vintage_kind IN ('exact_run', 'capture') AND f.member_kind = ? AND f.output_cadence = 3600
              AND {valid_members}
            QUALIFY row_number() OVER (PARTITION BY f.series_id, f.vintage_id, f.valid_at
                                      ORDER BY f.revision_no DESC) = 1
        )
        SELECT * FROM visible WHERE aligned
        QUALIFY count(*) OVER (PARTITION BY vintage_id, valid_at, metric, member_number) = 1
    """, [*contract["scope"], cutoff, "member" if member else "deterministic"])


def forecast(db, name, contract, start, end, cutoff):
    start, end = hour_grid(db, start, end)
    hours = contract.get("hours")
    if hours and end - start != timedelta(hours=hours):
        raise ValueError(f"The release contract requires a {hours}-hour window")
    ensemble = contract["kind"] == "ensemble"
    metrics_stage(db, contract, None if ensemble else FORECAST_COLUMNS)
    create_forecast_candidates(db, contract, cutoff)
    ranked = db.execute("""
        SELECT vintage_id, max(initialized_at), max(captured_at)
        FROM g_candidates
        GROUP BY vintage_id
        HAVING CASE WHEN ? THEN count(*) > 0 ELSE
            (count(*) FILTER (WHERE required AND usable) =
                (SELECT count(*) FROM g_hour) * (SELECT count(*) FROM g_metric WHERE required)
             AND count(*) FILTER (WHERE usable) > 0) END
        ORDER BY max(coalesce(initialized_at, captured_at)) DESC, max(warehouse_available_at) DESC, vintage_id DESC
        LIMIT 1
    """, [ensemble]).fetchone()
    if not ranked:
        return {"status": "ineligible", "release_id": None}
    vintage, initialized, captured = ranked
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_selected AS
        SELECT * FROM g_candidates WHERE vintage_id = ? AND (? OR usable)
    """, [vintage, ensemble])
    if ensemble:
        output = db.execute(f"""
            WITH settings AS (
                SELECT g_metric.*, measure_id FROM g_metric
                JOIN silver.measure USING (measure_code, definition_version)
                WHERE g_metric.unit = silver.measure.canonical_unit
            ), per_hour AS (
                SELECT m.measure_id, h.valid_at, len(m.roster) AS expected,
                       count(DISTINCT s.member_number) FILTER (WHERE s.usable) AS usable,
                       quantile_cont(s.value_number, [0.10, 0.50, 0.90]) FILTER (WHERE s.usable) AS q
                FROM settings m CROSS JOIN g_hour h
                LEFT JOIN g_selected s ON s.metric = m.metric AND s.valid_at = h.valid_at
                GROUP BY m.measure_id, h.valid_at, m.roster
            )
            SELECT measure_id, valid_at, CASE WHEN usable = expected THEN q[1] END,
                   CASE WHEN usable = expected THEN q[2] END, CASE WHEN usable = expected THEN q[3] END,
                   expected, usable, CASE WHEN expected IS NULL THEN 'unknown_roster'
                                          WHEN usable = expected THEN 'complete' ELSE 'partial' END
            FROM per_hour ORDER BY measure_id, valid_at
        """).fetchall()
        coverage = "complete" if all(row[-1] == "complete" for row in output) else "partial"
    else:
        output = db.execute(f"""
            SELECT h.valid_at, {pivot(FORECAST_COLUMNS)}
            FROM g_hour h LEFT JOIN g_selected s USING (valid_at)
            GROUP BY h.valid_at ORDER BY h.valid_at
        """).fetchall()
        coverage = "complete"
    manifest = manifest_from(db, "g_selected")
    release_id = digest([name, contract, start, end, manifest, output])
    if db.execute("SELECT 1 FROM gold.forecast_release WHERE release_id = ?", [release_id]).fetchone():
        return {"status": "unchanged", "release_id": release_id}
    scope = contract["scope"]
    if not ensemble:
        prior = db.execute(f"""
            SELECT r.window_start, coalesce(r.initialized_at, v.captured_at)
            FROM gold.forecast_hourly_latest f JOIN gold.forecast_release r USING (release_id)
            JOIN silver.vintage v ON r.vintage_id = v.vintage_id
            WHERE f.contract_version = ? AND {scope_filter()}
            LIMIT 1
        """, [contract["version"], *scope]).fetchone()
        if prior and (start < prior[0] or (initialized or captured) < prior[1]):
            return {"status": "superseded", "release_id": None}
    build_id, _ = build_record(db, name, contract, [start, end], output, manifest, cutoff,
                               {"coverage": coverage})
    required_at = db.execute("SELECT max(warehouse_available_at) FROM g_selected").fetchone()[0]
    insert(db, "gold.forecast_release", [
        release_id, contract["version"], *scope, start, end, vintage, initialized, required_at,
        max(now(), required_at), coverage, build_id])
    insert_many(db, "gold.release_input", [(release_id, *row) for row in manifest])
    if ensemble:
        insert_many(db, "gold.ensemble_hourly", [(release_id, *row) for row in output])
    else:
        scope_sql = scope_filter("gold.forecast_hourly_latest")
        db.execute(f"DELETE FROM gold.forecast_hourly_latest WHERE contract_version = ? AND {scope_sql}",
                   [contract["version"], *scope])
        insert_many(db, "gold.forecast_hourly_latest", [
            (contract["version"], *scope, at, release_id, temp, precip, wind, coverage)
            for at, temp, precip, wind in output])
    return {"status": "published", "release_id": release_id, "rows": len(output)}


def daily_sources_stage(db, contract, weather):
    metrics_stage(db, contract, WEATHER_COLUMNS if weather else None)
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_report_source AS
        SELECT *, 0 AS source_rank,
               CASE WHEN block_code = 'daily' THEN 'provider_daily' ELSE 'hourly' END AS report_source,
               ?::BIGINT AS site_id, ?::BIGINT AS product_id, ?::VARCHAR AS requested_model_selector,
               ?::BIGINT AS profile_id
        FROM g_metric
    """, contract["scope"])
    for code, scope in contract.get("provider_scopes", {}).items():
        provider = contract["metrics"][code]["provider"]
        db.execute("""
            INSERT INTO g_report_source BY NAME
            SELECT * REPLACE (? AS measure_code, ? AS definition_version, 'daily' AS block_code,
                             0 AS seconds_before, 0 AS seconds_after, 1 AS source_rank,
                             'provider_daily' AS report_source, ? AS site_id, ? AS product_id,
                             ? AS requested_model_selector, ? AS profile_id)
            FROM g_report_source WHERE metric = ? AND source_rank = 0
        """, [provider["measure"], provider.get("definition_version", 1), *scope, code])


def daily(db, name, contract, start, end, cutoff):
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    if end <= start:
        raise ValueError("Use a half-open reporting date range")
    weather = contract["kind"] == "weather_daily"
    daily_sources_stage(db, contract, weather)
    region = contract["timezone"]
    zone = ZoneInfo(region)
    calendar = []
    day = start
    while day < end:
        following = day + timedelta(days=1)
        calendar.append((day, local_midnight(day, zone), local_midnight(following, zone)))
        day = following
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_day (local_date DATE, day_start TIMESTAMPTZ, day_end TIMESTAMPTZ)
    """)
    insert_many(db, "g_day", calendar)
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE g_daily_options AS
        WITH expected AS (
            SELECT d.*, m.*, CASE WHEN seconds_before > 0 THEN slot + INTERVAL '1 hour' ELSE slot END AS valid_at
            FROM g_day d CROSS JOIN g_report_source m,
            LATERAL unnest(CASE WHEN m.block_code = 'daily' THEN [d.day_start]
                                ELSE range(d.day_start, d.day_end, INTERVAL '1 hour') END) AS clock(slot)
        ), visible AS (
            SELECT m.local_date, m.metric, m.source_rank, f.series_id, f.vintage_id, f.valid_at, f.revision_no,
                   f.value_number, {usable()} AS usable,
                   CASE WHEN m.block_code = 'daily' THEN f.window_start = m.day_start AND f.window_end = m.day_end
                        AND f.temporal_statistic = CASE m.reducer WHEN 'min' THEN 'minimum'
                            WHEN 'max' THEN 'maximum' WHEN 'avg' THEN 'mean' ELSE m.reducer END
                        ELSE {interval_join()} AND f.window_start >= m.day_start AND f.window_end <= m.day_end
                        END AS aligned
            FROM gold.series_values f JOIN expected m ON {metric_join()} AND {scope_join("f", "m")}
                                                       AND f.valid_at = m.valid_at
            WHERE f.warehouse_available_at <= ? AND f.member_kind = 'deterministic'
              AND (m.block_code = 'daily' OR f.output_cadence = 3600)
            QUALIFY row_number() OVER (PARTITION BY m.local_date, m.metric, m.source_rank,
                                                    f.series_id, f.vintage_id, f.valid_at
                                      ORDER BY f.revision_no DESC) = 1
        ), chosen_vintage AS (
            SELECT local_date, metric, source_rank, vintage_id FROM visible v JOIN silver.vintage vt USING (vintage_id)
            GROUP BY local_date, metric, source_rank, vintage_id
            QUALIFY row_number() OVER (PARTITION BY local_date, metric, source_rank
                ORDER BY count(*) FILTER (WHERE aligned AND usable) DESC,
                         max(coalesce(vt.initialized_at, vt.captured_at)) DESC NULLS LAST, vintage_id DESC) = 1
        ), matched AS (
            SELECT * FROM visible JOIN chosen_vintage USING (local_date, metric, source_rank, vintage_id)
            QUALIFY count(*) OVER (PARTITION BY local_date, metric, source_rank, valid_at) = 1
        )
        SELECT e.local_date, e.day_start, e.day_end, e.metric, e.unit, e.reducer, e.source_rank, e.report_source,
               s.* EXCLUDE (local_date, metric, source_rank),
               coalesce(s.usable AND s.aligned, false) AS complete
        FROM expected e LEFT JOIN matched s USING (local_date, metric, source_rank, valid_at)
    """, [cutoff])
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_daily_result AS
        WITH aggregates AS (
            SELECT local_date, day_start, day_end, metric, unit, source_rank, report_source,
                   count(*) AS expected_slots, count(*) FILTER (WHERE complete) AS usable_slots,
                   CASE WHEN bool_and(complete) THEN
                      CASE any_value(reducer) WHEN 'min' THEN min(value_number) WHEN 'max' THEN max(value_number)
                         WHEN 'sum' THEN sum(value_number) WHEN 'avg' THEN avg(value_number) END END AS value_number,
                   CASE WHEN bool_and(complete) THEN 'complete' ELSE 'partial' END AS coverage_status
            FROM g_daily_options GROUP BY ALL
        )
        SELECT * FROM aggregates
        QUALIFY row_number() OVER (PARTITION BY local_date, metric
            ORDER BY (report_source = 'provider_daily' AND coverage_status = 'complete') DESC, source_rank) = 1;
        CREATE OR REPLACE TEMP VIEW g_daily_input AS
        SELECT i.* FROM g_daily_options i JOIN g_daily_result r USING (local_date, metric, source_rank)
    """)
    if weather:
        output = db.execute(f"""
            SELECT local_date, day_start, day_end, {pivot(WEATHER_COLUMNS)},
                   to_json(map(list(metric ORDER BY metric),
                               list(struct_pack(expected := expected_slots, usable := usable_slots,
                                                status := coverage_status, source := report_source) ORDER BY metric)))
            FROM g_daily_result GROUP BY local_date, day_start, day_end ORDER BY local_date
        """).fetchall()
    else:
        output = db.execute("""
            SELECT local_date, metric, day_start, day_end, value_number, unit, expected_slots, usable_slots,
                   coverage_status FROM g_daily_result ORDER BY local_date, metric
        """).fetchall()
    partial = db.execute("SELECT count(*) FROM g_daily_result WHERE coverage_status <> 'complete'").fetchone()[0]
    manifest = manifest_from(db, "g_daily_input")
    build_id, created = build_record(db, name, contract, [start, end], output, manifest, cutoff,
                                    {"partial_metrics": partial})
    if not created:
        return {"status": "unchanged", "build_id": build_id}
    prefix = [contract["version"], *contract["scope"], region]
    table = f"gold.{contract['kind']}"
    db.execute(f"""
        DELETE FROM {table} WHERE contract_version = ? AND {scope_filter(table)}
        AND reporting_timezone = ? AND local_date >= ? AND local_date < ?
    """, [*prefix, start, end])
    insert_many(db, table, [(*prefix, *row, build_id) for row in output])
    return {"status": "published", "build_id": build_id, "rows": len(output)}


def training_candidates(db, contract, cutoff, basis, surface):
    if surface == "gold":
        db.execute(f"""
            CREATE OR REPLACE TEMP TABLE g_served AS
            SELECT d.decision_at, r.release_id, r.vintage_id, r.published_at, r.initialized_at
            FROM g_decision d JOIN gold.forecast_release r ON r.published_at <= d.decision_at
            JOIN ops.gold_build b USING (build_id)
            WHERE b.contract_name = ? AND r.contract_version = ? AND {scope_filter('r')}
            QUALIFY row_number() OVER (PARTITION BY d.decision_at
                       ORDER BY r.published_at DESC, r.release_id DESC) = 1
        """, [contract["release_contract"], contract["release_version"], *contract["scope"]])
        selection = """
            JOIN g_served r USING (decision_at)
            JOIN gold.release_input i ON i.release_id = r.release_id AND i.valid_at = e.input_at
            JOIN gold.series_values f ON (f.series_id, f.vintage_id, f.valid_at, f.revision_no) =
                                         (i.series_id, i.vintage_id, i.valid_at, i.revision_no)
        """
        release_at = "r.published_at"
        clock = "f.warehouse_available_at"
    else:
        selection = "JOIN gold.series_values f ON f.valid_at = e.input_at"
        release_at = "NULL::TIMESTAMPTZ"
        clock = "f.warehouse_available_at" if basis == "warehouse" else "f.source_available_at"
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE g_feature_inputs AS
        WITH visible AS (
            SELECT e.decision_at, e.valid_at AS target_at, f.valid_at, f.series_id, f.vintage_id, f.revision_no,
                   f.initialized_at, f.captured_at, m.metric, m.required, f.value_number,
                   m.lookback_hours, m.reducer,
                   greatest({clock}, {release_at}) AS available_at,
                   {usable()} AS usable, {interval_join()} AS aligned
            FROM g_feature_slots e {selection}
            JOIN g_metric m ON m.metric = e.metric AND {metric_join()}
            WHERE {scope_filter()} AND f.vintage_kind IN ('exact_run', 'capture')
              AND f.member_kind = 'deterministic' AND f.output_cadence = 3600
              AND {clock} <= e.decision_at AND f.warehouse_available_at <= ?
            QUALIFY row_number() OVER (PARTITION BY e.decision_at, e.valid_at, m.metric,
                                                    f.series_id, f.vintage_id, f.valid_at
                                      ORDER BY f.revision_no DESC) = 1
        )
        SELECT * FROM visible WHERE aligned AND usable
        QUALIFY count(*) OVER (PARTITION BY decision_at, vintage_id, metric, target_at, valid_at) = 1
    """, [*contract["scope"], cutoff])
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_feature_candidates AS
        SELECT decision_at, target_at AS valid_at, vintage_id, metric, any_value(required) AS required,
               max(initialized_at) AS initialized_at, max(captured_at) AS captured_at,
               max(available_at) AS available_at,
               CASE any_value(reducer) WHEN 'sum' THEN sum(value_number) WHEN 'min' THEN min(value_number)
                    WHEN 'max' THEN max(value_number) ELSE avg(value_number) END AS value_number
        FROM g_feature_inputs
        GROUP BY decision_at, target_at, vintage_id, metric
        HAVING count(*) = max(lookback_hours)
    """)
    if surface == "silver":
        db.execute("""
            CREATE OR REPLACE TEMP TABLE g_served AS
            WITH needed AS (
                SELECT decision_at, count(*) * (SELECT count(*) FROM g_metric WHERE required) AS slots
                FROM g_expected_feature GROUP BY decision_at
            ), ranked AS (
                SELECT f.decision_at, vintage_id, max(initialized_at) AS initialized_at,
                       count(*) FILTER (WHERE required) = max(needed.slots) AS complete,
                       max(coalesce(initialized_at, captured_at)) AS vintage_at,
                       max(available_at) AS latest_input
                FROM g_feature_candidates f JOIN needed USING (decision_at)
                GROUP BY f.decision_at, vintage_id
            )
            SELECT decision_at, vintage_id, initialized_at, NULL::VARCHAR AS release_id,
                   NULL::TIMESTAMPTZ AS published_at
            FROM ranked
            QUALIFY row_number() OVER (PARTITION BY decision_at
                    ORDER BY complete DESC, vintage_at DESC, latest_input DESC, vintage_id DESC) = 1
        """)
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_features AS
        SELECT c.* FROM g_feature_candidates c
        JOIN g_served s USING (decision_at, vintage_id);
        CREATE OR REPLACE TEMP VIEW g_selected_feature_inputs AS
        SELECT i.series_id, i.vintage_id, i.valid_at, i.revision_no
        FROM g_feature_inputs i JOIN g_features f
        ON (i.decision_at, i.target_at, i.vintage_id, i.metric) = (f.decision_at, f.valid_at, f.vintage_id, f.metric)
    """)


def training_labels(db, contract, cutoff):
    label = contract["label"]
    before, after = label.get("before", 0), label.get("after", 0)
    if before < 0 or after < 0 or (before and after):
        raise ValueError("Invalid target interval")
    db.execute(f"""
        CREATE OR REPLACE TEMP TABLE g_labels AS
        WITH visible AS (
            SELECT f.series_id, f.vintage_id, f.valid_at, f.revision_no, f.warehouse_available_at,
                   f.value_number, f.quality_status, f.window_start, f.window_end
            FROM gold.series_values f JOIN g_hour USING (valid_at)
            WHERE {scope_filter()} AND f.measure_code = ? AND f.definition_version = ?
              AND f.canonical_unit = ? AND f.block_code = 'hourly' AND f.output_cadence = 3600
              AND f.member_kind = 'deterministic' AND f.vintage_kind = 'archive'
              AND f.scenario_code IS NULL AND f.fixed_lead_hours IS NULL AND f.warehouse_available_at <= ?
            QUALIFY row_number() OVER (PARTITION BY f.series_id, f.vintage_id, f.valid_at
                                      ORDER BY f.revision_no DESC) = 1
        ), checked AS (
            SELECT *, CASE
                WHEN count(*) OVER (PARTITION BY valid_at) <> 1 THEN 'ambiguous'
                WHEN window_start <> valid_at - ? * INTERVAL '1 second'
                     OR window_end <> valid_at + ? * INTERVAL '1 second' THEN 'unaligned'
                WHEN value_number IS NULL OR quality_status <> 'valid' THEN 'unusable'
                ELSE 'complete' END AS label_status
            FROM visible
            QUALIFY row_number() OVER (PARTITION BY valid_at ORDER BY series_id, vintage_id) = 1
        )
        SELECT t.valid_at, CASE WHEN l.label_status = 'complete' THEN l.value_number END AS target,
               CASE WHEN l.label_status <> 'ambiguous' THEN l.warehouse_available_at END AS availability,
               CASE WHEN l.label_status <> 'ambiguous' THEN l.series_id END AS series_id,
               CASE WHEN l.label_status <> 'ambiguous' THEN l.vintage_id END AS vintage_id,
               CASE WHEN l.label_status <> 'ambiguous' THEN l.revision_no END AS revision_no,
               coalesce(l.label_status, 'missing') AS label_status
        FROM (SELECT DISTINCT valid_at FROM g_expected_feature) t LEFT JOIN checked l USING (valid_at)
    """, [*contract["label_scope"], label["measure"], label.get("definition_version", 1),
          label["unit"], cutoff, before, after])


def train(db, name, contract, start, end, *, decisions, dataset, cutoff, basis, surface):
    if not dataset or not decisions:
        raise ValueError("ML builds require a dataset ID and at least one decision time")
    if basis not in {"warehouse", "verified_source"} or surface not in {"silver", "gold"}:
        raise ValueError("Unknown feature surface or availability basis")
    if surface == "gold" and (basis != "warehouse" or not contract.get("release_version")):
        raise ValueError("The Gold surface requires warehouse availability and a pinned forecast release contract")
    start, end = hour_grid(db, start, end)
    decisions = sorted(set(instant(x) for x in decisions))
    existing = db.execute("""
        SELECT d.build_cutoff_at, b.quality_results, d.build_id FROM gold.ml_dataset d
        JOIN ops.gold_build b USING (build_id) WHERE d.dataset_id = ?
    """, [dataset]).fetchone()
    cutoff = instant(cutoff) if cutoff else existing[0] if existing else now()
    settings = digest([name, contract, dataset, start, end, decisions, cutoff, basis, surface])
    if existing:
        if json.loads(existing[1]).get("dataset_specification") != settings:
            raise ValueError("This dataset ID is frozen. Use a new ID for a new build")
        return {"status": "unchanged", "build_id": existing[2], "dataset_id": dataset}
    if any(decision > cutoff or decision >= end for decision in decisions):
        raise ValueError("Each decision requires target hours and must be at or before the build cutoff")
    metrics_stage(db, contract, FEATURE_COLUMNS)
    db.execute("CREATE OR REPLACE TEMP TABLE g_decision (decision_at TIMESTAMPTZ PRIMARY KEY)")
    insert_many(db, "g_decision", [(decision,) for decision in decisions])
    db.execute("""
        CREATE OR REPLACE TEMP TABLE g_expected_feature AS
        SELECT decision_at, valid_at FROM g_decision CROSS JOIN g_hour WHERE valid_at >= decision_at;
        CREATE OR REPLACE TEMP TABLE g_feature_slots AS
        SELECT e.*, m.metric, e.valid_at - lag * INTERVAL '1 hour' AS input_at
        FROM g_expected_feature e CROSS JOIN g_metric m, LATERAL range(m.lookback_hours) AS slots(lag)
    """)
    training_candidates(db, contract, cutoff, basis, surface)
    training_labels(db, contract, cutoff)
    features = db.execute(f"""
        SELECT e.decision_at, e.valid_at, s.vintage_id, s.release_id, s.initialized_at,
               greatest(max(c.available_at), s.published_at),
               epoch(e.valid_at - s.initialized_at) / 3600, epoch(e.valid_at - e.decision_at) / 3600,
               {pivot(FEATURE_COLUMNS, "c")},
               count(c.metric) FILTER (WHERE m.required) < (SELECT count(*) FROM g_metric WHERE required),
               coalesce(to_json(map(
                   list(m.metric ORDER BY m.metric) FILTER (WHERE m.lookback_hours > 1),
                   list(c.value_number ORDER BY m.metric) FILTER (WHERE m.lookback_hours > 1)
               )), '{{}}'::JSON),
               to_json(map(list(m.metric ORDER BY m.metric), list(c.metric IS NULL ORDER BY m.metric)))
        FROM g_expected_feature e
        CROSS JOIN g_metric m
        LEFT JOIN g_served s USING (decision_at)
        LEFT JOIN g_features c ON c.decision_at = e.decision_at AND c.valid_at = e.valid_at AND c.metric = m.metric
        GROUP BY e.decision_at, e.valid_at, s.vintage_id, s.release_id, s.initialized_at, s.published_at
        ORDER BY e.decision_at, e.valid_at
    """).fetchall()
    labels = db.execute("SELECT * FROM g_labels ORDER BY valid_at").fetchall()
    manifest = manifest_from(db, "g_selected_feature_inputs", "g_labels")
    build_id, _ = build_record(db, name, contract, settings, [*features, *labels], manifest, cutoff, {
        "dataset_specification": settings, "feature_rows": len(features), "label_rows": len(labels),
        "missing_feature_rows": sum(row[-3] for row in features),
        "missing_label_rows": sum(row[-1] != "complete" for row in labels),
    })
    insert(db, "gold.ml_dataset", [dataset, contract["feature_set_version"], contract["label_set_version"],
                                    basis, surface, cutoff, build_id])
    insert_many(db, "gold.ml_forecast_features", [(dataset, *contract["scope"], *row) for row in features])
    site_id, label_product, _, label_profile = contract["label_scope"]
    insert_many(db, "gold.ml_weather_labels", [
        (dataset, site_id, at, target, label_product, label_profile, avail, series, vintage, revision, status)
        for at, target, avail, series, vintage, revision, status in labels])
    return {"status": "published", "build_id": build_id, "dataset_id": dataset,
            "features": len(features), "labels": len(labels)}


def publish(db, config, name, start, end, *, cutoff=None, decisions=(), dataset=None,
            basis="warehouse", surface="silver"):
    with transaction(db):
        contract = gold_contract(db, config, name)
        kind = contract["kind"]
        if kind == "ml":
            result = train(db, name, contract, start, end, decisions=decisions, dataset=dataset,
                           cutoff=cutoff, basis=basis, surface=surface)
        elif kind in {"forecast", "ensemble"}:
            result = forecast(db, name, contract, start, end, instant(cutoff) if cutoff else now())
        elif kind in {"weather_daily", "environment_daily"}:
            result = daily(db, name, contract, start, end, instant(cutoff) if cutoff else now())
        else:
            raise ValueError(f"Unknown Gold contract kind {kind}")
    return result


def impacted_days(db, contract, after, through):
    intervals = db.execute(f"""
        WITH changed AS (
            SELECT f.series_id, f.vintage_id, f.valid_at, f.revision_no, f.window_start, f.window_end
            FROM gold.series_values f JOIN g_report_source m ON {metric_join()} AND {scope_join("f", "m")}
            WHERE f.warehouse_available_at <= ?
              AND (?::TIMESTAMPTZ IS NULL OR f.warehouse_available_at > ?) AND f.member_kind = 'deterministic'
        )
        SELECT DISTINCT window_start, window_end FROM changed
        UNION
        SELECT DISTINCT old.window_start, old.window_end
        FROM changed c JOIN silver.series_value old
        ON (c.series_id, c.vintage_id, c.valid_at, c.revision_no - 1)
           = (old.series_id, old.vintage_id, old.valid_at, old.revision_no)
    """, [through, after, after]).fetchall()
    zone = ZoneInfo(contract["timezone"])
    lower = date.fromisoformat(str(contract["reporting_start"])) if "reporting_start" in contract else date.min
    upper = date.fromisoformat(str(contract["reporting_end"])) if "reporting_end" in contract else date.max
    affected = set()
    for start, end in intervals:
        day = max(lower, start.astimezone(zone).date())
        # A point measurement belongs to its date. A period intersects dates before its exclusive end.
        last = (max(start, end - timedelta(microseconds=1))).astimezone(zone).date()
        while day <= last and day < upper:
            affected.add(day)
            day += timedelta(days=1)
    days = sorted(affected)
    if not days:
        return
    first = previous = days[0]
    for day in days[1:]:
        if day != previous + timedelta(days=1):
            yield first, previous + timedelta(days=1)
            first = day
        previous = day
    yield first, previous + timedelta(days=1)


def refresh(db, config, names=None):
    results = {}
    selected = names or [name for name, contract in config["contracts"].items() if contract["kind"] != "ml"]
    for name in selected:
        with transaction(db):
            contract = gold_contract(db, config, name)
            kind = contract["kind"]
            cutoff = now()
            if kind in {"forecast", "ensemble"}:
                window = db.execute("""
                    SELECT window_start, window_end FROM ops.extraction_slice
                    WHERE json_extract_string(specification, '$.job_name') = ?
                      AND window_end - window_start = ? * INTERVAL '1 hour'
                      AND status IN ('complete', 'partial', 'retryable')
                    ORDER BY window_start DESC, window_end DESC LIMIT 1
                """, [contract["job"], contract["hours"]]).fetchone()
                results[name] = forecast(db, name, contract, *window, cutoff) if window else {"status": "no_input"}
            elif kind in {"weather_daily", "environment_daily"}:
                version = contract["version"]
                previous = db.execute("""
                    SELECT processed_through FROM ops.gold_refresh WHERE contract_name = ? AND contract_version = ?
                """, [name, version]).fetchone()
                daily_sources_stage(db, contract, kind == "weather_daily")
                windows = list(impacted_days(db, contract, previous[0] if previous else None, cutoff))
                results[name] = [daily(db, name, contract, start, end, cutoff) for start, end in windows]
                db.execute("""
                    INSERT INTO ops.gold_refresh VALUES (?, ?, ?)
                    ON CONFLICT (contract_name, contract_version)
                    DO UPDATE SET processed_through = greatest(excluded.processed_through, gold_refresh.processed_through)
                """, [name, version, cutoff])
            else:
                raise ValueError("ML datasets require an explicit publish with decisions and a dataset ID")
    return results
