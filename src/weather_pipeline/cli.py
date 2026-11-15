import argparse
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .bronze import fetch_capture, plan_capture, request_plan, writer_lock
from .publish import build
from .silver import read_capture


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build the local Berlin weather outlook")
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture", help="Fetch Open-Meteo and publish a live outlook")
    capture.add_argument("--output", type=Path, default=Path("data/live"))
    capture.add_argument("--resume", metavar="CAPTURE_ID", help="Continue an existing live capture")
    replay = commands.add_parser("replay", help="Build an outlook from saved Bronze responses")
    replay_source = replay.add_mutually_exclusive_group()
    replay_source.add_argument("--fixture", type=Path, help="Recorded fixture directory")
    replay_source.add_argument("--capture-dir", type=Path, help="Saved live Bronze capture directory")
    replay.add_argument("--output", type=Path, default=Path("data/replay"))
    args = parser.parse_args(argv)

    try:
        with writer_lock(args.output):
            if args.command == "capture":
                if args.resume:
                    if not re.fullmatch(r"\d{8}T\d{6}Z-[a-f0-9]{8}", args.resume):
                        raise ValueError("Invalid capture ID")
                    source = args.output / "raw" / args.resume
                    manifest = read_capture(source)
                    if args.resume != manifest["capture_id"] or manifest["requests"] != request_plan(
                            args.resume, manifest["window"]):
                        raise ValueError("Saved requests differ from the current Berlin request plan")
                    if datetime.fromisoformat(manifest["deadline_utc"]) <= datetime.now(UTC):
                        print("Capture deadline has passed. Building from available Bronze responses.", file=sys.stderr)
                else:
                    source, manifest = plan_capture(args.output)
                print(f"Capturing {manifest['capture_id']} to {source.resolve()}", flush=True)
                fetch_capture(source, manifest)
            else:
                source = args.capture_dir or args.fixture or Path("fixtures/berlin")

            summary, destination = build(source, args.output, recorded=args.command == "replay")
            print(f"{summary['status'].upper()} · build {summary['build_id']}")
            if summary["status"] == "blocked":
                print(f"Diagnostics: {(destination / 'quality_issues.csv').resolve()}", file=sys.stderr)
                return 2
            print(f"Report: {(destination / 'outlook.html').resolve()}")
            return 0
    except (OSError, KeyError, TypeError, ValueError, duckdb.Error) as error:
        print(f"Pipeline error: {error}", file=sys.stderr)
        return 1
