import argparse
import sys
import uuid
from pathlib import Path

import duckdb
import httpx

from .bronze import Response, checksum, land, poll, work
from .db import initialize, instant, json_text, now, uuid_key
from .gold import publish, refresh
from .operations import benchmark, snapshot
from .plan import get_slice, load_config, plan_job, replay_mappings
from .silver import normalize


def parser():
    root = argparse.ArgumentParser(description="Open-Meteo warehouse")
    root.add_argument("--db", default="weather.duckdb", help="DuckDB file")
    root.add_argument("--config", type=Path, help="TOML inventory and contracts (default: packaged starter inventory)")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Create the warehouse schemas")
    plan = commands.add_parser("plan", help="Queue a configured job")
    plan.add_argument("job")
    plan.add_argument("--anchor", type=instant, help="Clock for a reproducible rolling window")
    plan.add_argument("--start", help="Inclusive start date or timestamp")
    plan.add_argument("--end", help="Exclusive end date or timestamp")
    plan.add_argument("--run", type=instant, help="Exact model initialization")
    group = commands.add_parser("plan-group", help="Queue the jobs in a configured extraction group")
    group.add_argument("group")
    group.add_argument("--anchor", type=instant)
    worker = commands.add_parser("work", help="Fetch queued slices in priority order")
    worker.add_argument("--max-requests", default=20, type=positive)
    worker.add_argument("--max-attempts", default=3, type=positive)
    worker.add_argument("--workers", default=4, type=positive)
    model = commands.add_parser("poll", help="Poll a model and schedule new slices")
    model.add_argument("job")
    ingest = commands.add_parser("ingest", help="Land and normalize saved HTTP response bytes")
    ingest.add_argument("slice_id", type=uuid.UUID)
    ingest.add_argument("body", type=Path)
    ingest.add_argument("--requested-at", required=True, type=instant)
    ingest.add_argument("--fetched-at", required=True, type=instant)
    ingest.add_argument("--fetch-id", type=uuid.UUID)
    ingest.add_argument("--sha256")
    ingest.add_argument("--http-status", type=int, default=200)
    ingest.add_argument("--content-type", default="application/json")
    ingest.add_argument("--error")
    ingest.add_argument("--source-available-at", type=instant)
    ingest.add_argument("--source-reference", help="Citation or identifier of evidence for the exact response version")
    ingest.add_argument("--ordered-correction", action="store_true")
    replay = commands.add_parser("replay", help="Revalidate a landed attempt")
    replay.add_argument("slice_id", type=uuid.UUID)
    replay.add_argument("fetch_id", type=uuid.UUID)
    replay.add_argument("--mapping-job", help="Replay with updated mapping versions from a compatible configured job")
    gold = commands.add_parser("publish", help="Build a versioned Gold contract")
    gold.add_argument("contract")
    gold.add_argument("--start", required=True, help="Inclusive UTC hour or local reporting date")
    gold.add_argument("--end", required=True, help="Exclusive UTC hour or local reporting date")
    gold.add_argument("--cutoff", type=instant)
    gold.add_argument("--decision", action="append", type=instant, default=[])
    gold.add_argument("--dataset")
    gold.add_argument("--basis", choices=("warehouse", "verified_source"), default="warehouse")
    gold.add_argument("--surface", choices=("silver", "gold"), default="silver")
    update = commands.add_parser("refresh", help="Refresh affected reporting and forecast outputs")
    update.add_argument("contract", nargs="*")
    export = commands.add_parser("snapshot", help="Publish a versioned read-only DuckDB copy")
    export.add_argument("--output", type=Path, default=Path("data/snapshots"))
    bench = commands.add_parser("benchmark", help="Measure ingestion, freshness, and DuckDB serving queries")
    bench.add_argument("--trials", type=positive)
    bench.add_argument("--dataset")
    commands.add_parser("status", help="List planned windows, states, and coverage")
    return root


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def show(result):
    print(json_text(result))


def run(args, db):
    command = args.command
    if command == "init":
        show({"database": str(Path(args.db).absolute())})
    elif command in {"plan", "plan-group", "poll", "publish", "refresh"}:
        config = load_config(args.config)
        if command == "plan":
            ids = plan_job(db, config, args.job, anchor=args.anchor, start=args.start, end=args.end, run=args.run)
            show({"slice_ids": ids})
        elif command == "plan-group":
            anchor = args.anchor or now()
            jobs = config["job_groups"][args.group]
            show(
                {
                    "inventory_version": config.get("warehouse", {}).get("version"),
                    "planned_slices": {job: len(plan_job(db, config, job, anchor=anchor)) for job in jobs},
                }
            )
        elif command == "poll":
            with httpx.Client(timeout=25) as client:
                status, ids = poll(db, client, config, args.job)
            show({"status": status, "slice_ids": ids})
            return 2 if status in {"invalid", "deferred_quota"} else 0
        elif command == "refresh":
            show(refresh(db, config, args.contract))
        else:
            result = publish(
                db,
                config,
                args.contract,
                args.start,
                args.end,
                cutoff=args.cutoff,
                decisions=args.decision,
                dataset=args.dataset,
                basis=args.basis,
                surface=args.surface,
            )
            show(result)
            return 2 if result["status"] in {"ineligible", "superseded"} else 0
    elif command == "snapshot":
        show(snapshot(db, args.output))
    elif command == "work":
        with httpx.Client(timeout=25) as client:
            results = work(
                db, client, max_requests=args.max_requests, max_attempts=args.max_attempts, workers=args.workers
            )
        show([{"slice_id": slice_id, "status": status} for slice_id, status in results])
        return 2 if any(status in {"failed", "invalid", "partial", "deferred_quota"} for _, status in results) else 0
    elif command in {"ingest", "replay"}:
        slice_ = get_slice(db, args.slice_id)
        fetch_id = args.fetch_id
        if command == "ingest":
            evidence = None
            if args.source_available_at and args.source_reference:
                evidence = (args.source_available_at, args.source_reference, args.ordered_correction)
            elif args.source_available_at or args.source_reference or args.ordered_correction:
                raise ValueError("Provide both a verified availability timestamp and its source reference")
            body = args.body.read_bytes()
            response = Response(args.http_status, args.content_type, body, args.error, args.fetched_at)
            fetch_id = fetch_id or uuid_key(
                [
                    args.slice_id,
                    args.requested_at,
                    args.fetched_at,
                    args.http_status,
                    args.error,
                    args.content_type,
                    checksum(body),
                ]
            )
            fetch_id = land(
                db,
                slice_,
                response,
                requested_at=args.requested_at,
                fetch_id=fetch_id,
                expected_sha256=args.sha256,
                evidence=evidence,
            )
        if command == "replay" and args.mapping_job:
            slice_ = replay_mappings(slice_, load_config(args.config), args.mapping_job)
        status = normalize(db, slice_, fetch_id)
        show({"slice_id": args.slice_id, "fetch_id": fetch_id, "status": status})
        return 2 if status in {"failed", "invalid", "partial"} else 0
    elif command == "status":
        result = db.execute("""
            SELECT slice_id, api_product, requested_model_selector, window_start, window_end, status,
                   coverage_result, not_before_at FROM ops.extraction_slice ORDER BY priority, window_start, slice_id
        """)
        columns = [entry[0] for entry in result.description]
        show([dict(zip(columns, row, strict=True)) for row in result.fetchall()])
    return 0


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "benchmark":
            with duckdb.connect(args.db, read_only=True) as db:
                db.execute("SET TimeZone = 'UTC'")
                show(benchmark(db, load_config(args.config), trials=args.trials, dataset=args.dataset))
            return 0
        with initialize(args.db) as db:
            return run(args, db)
    except (ValueError, KeyError, OSError, duckdb.Error) as exc:
        print(f"weather-warehouse: {exc}", file=sys.stderr)
        return 1
