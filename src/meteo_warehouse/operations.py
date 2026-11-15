import hashlib
import math
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

import duckdb

from .db import CATEGORIES, json_text, now


TRAINING_EXPORT = """
    SELECT f.site_id, f.decision_at, f.valid_at, f.lead_hours, f.horizon_hours, f.forecast_temp_c,
           f.forecast_precip_mm, f.rolling_features, f.missing_features, l.target_temp_c
    FROM gold.ml_forecast_features f
    JOIN gold.ml_weather_labels l USING (dataset_id, site_id, valid_at)
    JOIN gold.ml_dataset d USING (dataset_id)
    WHERE f.dataset_id = ? AND f.feature_available_at <= f.decision_at
          AND l.label_available_at <= d.build_cutoff_at AND NOT f.missing_required_features
          AND l.target_temp_c IS NOT NULL
    ORDER BY f.site_id, f.decision_at, f.valid_at
"""


def snapshot(db, directory):
    path, = db.execute("SELECT path FROM duckdb_databases() WHERE database_name = current_database()").fetchone()
    if path is None or path == ":memory:":
        raise ValueError("Snapshots require a persistent warehouse file")
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    # This process holds DuckDB's write lock and issues no writes while copying the checkpointed file.
    db.execute("CHECKPOINT")
    with TemporaryDirectory(prefix=".snapshot-", dir=directory) as temporary:
        stage = Path(temporary) / "warehouse.duckdb"
        with open(path, "rb") as source, stage.open("wb") as target:
            shutil.copyfileobj(source, target)
            target.flush()
            os.fsync(target.fileno())
        with stage.open("rb") as source:
            version = hashlib.file_digest(source, "sha256").hexdigest()
        with duckdb.connect(str(stage), read_only=True) as reader:
            reader.execute("SELECT count(*) FROM ops.gold_build").fetchone()
        final = directory / f"weather-{version}.duckdb"
        if final.exists():
            with final.open("rb") as old:
                if hashlib.file_digest(old, "sha256").hexdigest() != version:
                    raise ValueError(f"Snapshot checksum mismatch for {final.name}")
        else:
            stage.chmod(0o444)
            stage.replace(final)
        metadata = {"version": version, "file": final.name, "bytes": final.stat().st_size, "created_at": now()}
        manifest = Path(temporary) / "latest.json"
        with manifest.open("w", encoding="utf-8") as stream:
            stream.write(json_text(metadata) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        manifest.replace(directory / "latest.json")
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return {**metadata, "path": str(final)}


def freshness(db):
    attempts = " UNION ALL ".join(
        f"SELECT fetch_id, slice_id, fetched_at FROM bronze.{category}" for category in CATEGORIES)
    rows = db.execute(f"""
        WITH per_release AS (
            SELECT r.release_id, r.published_at, max(a.fetched_at) AS fetched_at,
                   CASE WHEN count(f.source_available_at) = count(*) THEN max(f.source_available_at)
                        END AS source_ready,
                   max(o.source_available_at) AS exact_run_model_ready
            FROM gold.forecast_release r JOIN gold.release_input i USING (release_id)
            JOIN silver.series_value f
              ON (f.series_id, f.vintage_id, f.valid_at, f.revision_no) =
                 (i.series_id, i.vintage_id, i.valid_at, i.revision_no)
            JOIN ({attempts}) a ON a.fetch_id = f.fetch_id
            JOIN ops.extraction_slice e USING (slice_id)
            LEFT JOIN silver.model_update_observation o
                ON e.trigger_poll_id = o.poll_id AND r.initialized_at = o.initialized_at
            GROUP BY r.release_id, r.published_at
        ), clocks AS (
            SELECT epoch(published_at - fetched_at) AS received,
                   epoch(published_at - source_ready) AS verified,
                   epoch(published_at - exact_run_model_ready) AS feed
            FROM per_release
        )
        SELECT count(received), quantile_cont(received, 0.95),
               count(verified), quantile_cont(verified, 0.95),
               count(feed), quantile_cont(feed, 0.95) FROM clocks
    """).fetchone()
    return {clock: {"releases": count, "p95_seconds": round(value, 3) if value is not None else None}
            for clock, count, value in zip(("received_to_gold", "verified_source_to_gold", "exact_run_feed_to_gold"),
                                          rows[::2], rows[1::2])}


def throughput(db):
    rows = db.execute("""
        SELECT phase, sum(attempts), sum(usable_attempts), sum(new_facts),
               sum(epoch(finished_at - started_at)) AS active_seconds
        FROM ops.ingestion_batch GROUP BY phase
    """).fetchall()
    result = dict.fromkeys(("live", "backfill"))
    for phase, attempts, usable, facts, seconds in rows:
        result[phase] = {"attempts": attempts, "usable_attempts": usable, "new_facts": facts,
                         "active_seconds": round(seconds, 3),
                         "attempts_per_second": round(attempts / seconds, 2) if seconds else None,
                         "facts_per_second": round(facts / seconds, 2) if seconds else None}
    return result


def time_query(run, trials):
    rows = run()
    if not rows:
        return {"status": "no_data", "rows": 0}
    samples = []
    for _ in range(trials):
        started = perf_counter()
        run()
        samples.append((perf_counter() - started) * 1000)
    return {"status": "measured", "rows": rows, "trials": trials,
            "p95_ms": round(sorted(samples)[math.ceil(0.95 * trials) - 1], 3)}


def benchmark(db, config, *, trials=None, dataset=None):
    settings = config["benchmark"]
    locations = config.get("warehouse", {}).get("geography", list(config["sites"]))
    reporting_version = settings.get("reporting_version") or config["contracts"]["weather_daily"]["version"]
    trials = trials if trials is not None else settings["trials"]
    if type(trials) is not int or trials < 1:
        raise ValueError("Benchmark trials must be a positive integer")
    if dataset is None:
        latest = db.execute("SELECT dataset_id FROM gold.ml_dataset ORDER BY build_cutoff_at DESC, dataset_id LIMIT 1").fetchone()
        dataset = latest[0] if latest else None
    daily = time_query(lambda: len(db.execute("""
        SELECT site_id, local_date, temp_min_c, temp_max_c, precip_mm, wind_gust_max_ms, metric_coverage
        FROM gold.weather_daily JOIN silver.site USING (site_id)
        WHERE contract_version = ? AND local_date >= ? AND local_date < ?
          AND location_key IN (SELECT unnest(?))
        ORDER BY site_id, local_date
    """, [reporting_version, settings["daily_start"], settings["daily_end"], locations]).fetchall()), trials)
    history = time_query(lambda: len(db.execute("""
        WITH latest AS (
            SELECT valid_at, site_id, measure_code, value_number, quality_status
            FROM gold.series_values
            WHERE api_product = ? AND valid_at >= ? AND valid_at < ?
              AND location_key IN (SELECT unnest(?))
            QUALIFY row_number() OVER (PARTITION BY series_id, vintage_id, valid_at ORDER BY revision_no DESC) = 1
        )
        SELECT date_trunc('month', valid_at) AS month, site_id, measure_code, count(*) AS slots, avg(value_number)
        FROM latest WHERE quality_status = 'valid'
        GROUP BY month, site_id, measure_code ORDER BY month, site_id, measure_code
    """, [settings["history_product"], settings["history_start"], settings["history_end"], locations]).fetchall()), trials)
    with TemporaryDirectory(prefix="warehouse-benchmark-") as directory:
        file = Path(directory) / "training.parquet"
        def export():
            file.unlink(missing_ok=True)
            db.sql(TRAINING_EXPORT, params=[dataset]).write_parquet(str(file))
            return db.execute("SELECT count(*) FROM read_parquet(?)", [str(file)]).fetchone()[0]

        training = time_query(export, trials) if dataset else {"status": "no_dataset", "rows": 0}
    return {"inventory_version": config.get("warehouse", {}).get("version"),
            "query_windows": settings, "dataset_id": dataset, "freshness": freshness(db),
            "throughput": throughput(db), "query_latency": {
                "daily_report": daily, "multi_year_aggregation": history, "training_parquet_export": training}}
