import hashlib
import io
import json
import os
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import duckdb
import httpx

from meteo_warehouse.bronze import fetch_curl
from meteo_warehouse.cli import main

START = datetime(2026, 4, 22, 12, tzinfo=UTC)
END = START + timedelta(hours=24)


class CliTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.database = self.directory / "weather.duckdb"

    def invoke(self, *arguments, expected=0):
        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            result = main(["--db", str(self.database), *arguments])
        self.assertEqual(result, expected, errors.getvalue())
        if expected == 1:
            self.assertEqual(output.getvalue(), "")
            return errors.getvalue()
        self.assertEqual(errors.getvalue(), "")
        return json.loads(output.getvalue())

    def plan_forecast(self):
        result = self.invoke("plan", "forecast", "--start", START.isoformat(), "--end", END.isoformat())
        (slice_id,) = result["slice_ids"]
        return slice_id

    def response_bytes(self):
        fields = {
            "time": [int((START + timedelta(hours=hour)).timestamp()) for hour in range(24)],
            "temperature_2m": [10.0] * 24,
            "precipitation": [0.0] * 24,
            "wind_speed_10m": [18.0] * 24,
        }
        return json.dumps(
            {
                "latitude": 52.49,
                "longitude": 13.42,
                "timezone": "GMT",
                "utc_offset_seconds": 0,
                "hourly": fields,
                "hourly_units": {"temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h"},
            }
        ).encode()

    def test_module_and_console_entry_points_initialize_and_read_status(self):
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        result = subprocess.run(
            [sys.executable, "-m", "meteo_warehouse", "--db", str(self.database), "init"],
            capture_output=True,
            text=True,
            env=environment,
            check=True,
        )
        self.assertTrue(Path(json.loads(result.stdout)["database"]).samefile(self.database))
        launcher = Path(sys.executable).parent / "weather-warehouse"
        result = subprocess.run(
            [str(launcher), "--db", str(self.database), "status"],
            capture_output=True,
            text=True,
            env=environment,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout), [])

    def test_help_and_invalid_arguments_exit_before_creating_database(self):
        for arguments, expected in [
            (["--help"], 0),
            (["work", "--workers", "0"], 2),
            (["plan", "forecast", "--anchor", "2026-04-22T12:00:00"], 2),
            (["unknown-command"], 2),
        ]:
            with self.subTest(arguments=arguments):
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        main(["--db", str(self.database), *arguments])
                self.assertEqual(raised.exception.code, expected)
                self.assertFalse(self.database.exists())

    def test_planning_status_and_unaligned_window_error(self):
        self.invoke("init")
        self.assertEqual(self.invoke("status"), [])
        error = self.invoke(
            "plan",
            "forecast",
            "--start",
            "2026-04-22T12:30:00Z",
            "--end",
            "2026-04-23T12:30:00Z",
            expected=1,
        )
        self.assertIn("align", error)
        self.assertEqual(self.invoke("status"), [])
        slice_id = self.plan_forecast()
        (status,) = self.invoke("status")
        self.assertEqual(status["slice_id"], slice_id)
        self.assertEqual(status["status"], "queued")

    def test_unknown_job_and_missing_configuration_report_errors(self):
        self.assertIn("missing-job", self.invoke("plan", "missing-job", expected=1))
        self.assertIn(
            "missing.toml",
            self.invoke(
                "--config",
                str(self.directory / "missing.toml"),
                "plan",
                "forecast",
                expected=1,
            ),
        )
        self.assertEqual(self.invoke("status"), [])

    def test_plan_group_queues_reference_jobs(self):
        result = self.invoke("plan-group", "reference", "--anchor", START.isoformat())
        self.assertEqual(set(result["planned_slices"]), {"geocoding", "elevation"})
        self.assertTrue(all(count > 0 for count in result["planned_slices"].values()))
        self.assertEqual(len(self.invoke("status")), sum(result["planned_slices"].values()))

    def test_worker_publish_refresh_snapshot_and_benchmark(self):
        slice_id = self.plan_forecast()
        client = httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=self.response_bytes()))
        )
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            result = self.invoke("work", "--max-requests", "1", "--workers", "1")
        self.assertEqual(result, [{"slice_id": slice_id, "status": "complete"}])
        release = self.invoke("publish", "forecast", "--start", START.isoformat(), "--end", END.isoformat())
        self.assertEqual(release["status"], "published")
        self.assertEqual(release["rows"], 24)
        self.assertEqual(self.invoke("refresh", "forecast")["forecast"]["status"], "unchanged")
        with duckdb.connect(str(self.database), read_only=True) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM gold.forecast_hourly_latest").fetchone()[0], 24)
        snapshot = self.invoke("snapshot", "--output", str(self.directory / "snapshots"))
        self.assertTrue(Path(snapshot["path"]).is_file())
        measured = self.invoke("benchmark", "--trials", "1")
        self.assertEqual(measured["query_latency"]["daily_report"]["status"], "no_data")

    def test_ingest_and_replay_saved_response_keep_fact_count(self):
        slice_id = self.plan_forecast()
        body = self.response_bytes()
        saved = self.directory / "response.json"
        saved.write_bytes(body)
        result = self.invoke(
            "ingest",
            slice_id,
            str(saved),
            "--requested-at",
            START.isoformat(),
            "--fetched-at",
            (START + timedelta(minutes=5)).isoformat(),
            "--source-available-at",
            (START + timedelta(minutes=4)).isoformat(),
            "--source-reference",
            "CLI test fixture",
            "--sha256",
            hashlib.sha256(body).hexdigest(),
        )
        self.assertEqual(result["status"], "complete")
        replayed = self.invoke("replay", slice_id, result["fetch_id"], "--mapping-job", "forecast")
        self.assertEqual(replayed["status"], "complete")
        with duckdb.connect(str(self.database), read_only=True) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 72)

    def test_ingest_rejects_incomplete_availability_evidence(self):
        slice_id = self.plan_forecast()
        saved = self.directory / "response.json"
        saved.write_bytes(self.response_bytes())
        error = self.invoke(
            "ingest",
            slice_id,
            str(saved),
            "--requested-at",
            START.isoformat(),
            "--fetched-at",
            (START + timedelta(minutes=5)).isoformat(),
            "--source-available-at",
            (START + timedelta(minutes=4)).isoformat(),
            expected=1,
        )
        self.assertIn("Provide both", error)

    def test_ingest_without_evidence_reports_checksum_mismatch(self):
        slice_id = self.plan_forecast()
        body = self.response_bytes()
        saved = self.directory / "response.json"
        saved.write_bytes(body)
        result = self.invoke(
            "ingest",
            slice_id,
            str(saved),
            "--requested-at",
            START.isoformat(),
            "--fetched-at",
            (START + timedelta(minutes=5)).isoformat(),
            "--sha256",
            "0" * 64,
            expected=2,
        )
        self.assertEqual(result["status"], "invalid")
        with duckdb.connect(str(self.database), read_only=True) as db:
            self.assertEqual(db.execute("SELECT response_body FROM bronze.weather_forecast").fetchone()[0], body)
            self.assertEqual(db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 0)

    def test_worker_failure_and_ineligible_publication_use_status_two(self):
        self.plan_forecast()
        client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(500, content=b"unavailable")))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            result = self.invoke("work", "--max-requests", "1", "--max-attempts", "1", expected=2)
        self.assertEqual(result[0]["status"], "failed")
        result = self.invoke("publish", "forecast", "--start", START.isoformat(), "--end", END.isoformat(), expected=2)
        self.assertEqual(result["status"], "ineligible")
        self.assertEqual(self.invoke("refresh", "forecast", expected=2)["forecast"]["status"], "no_input")

    def test_worker_retries_connection_timeout_with_curl_and_publishes(self):
        slice_id = self.plan_forecast()
        clock = START

        def sleep(seconds):
            nonlocal clock
            clock += timedelta(seconds=seconds)

        def curl(command, **kwargs):
            Path(command[command.index("--output") + 1]).write_bytes(self.response_bytes())
            Path(command[command.index("--dump-header") + 1]).write_text(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n"
            )
            self.assertEqual(command[command.index("--proto") + 1], "=https")
            self.assertNotIn("--insecure", command)
            self.assertNotIn("-k", command)
            return subprocess.CompletedProcess(command, 0, stdout=b"200", stderr=b"")

        handler = Mock(side_effect=httpx.ConnectTimeout("TLS handshake timed out"))
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with (
            patch("meteo_warehouse.cli.httpx.Client", return_value=client),
            patch("meteo_warehouse.bronze.now", side_effect=lambda: clock),
            patch("meteo_warehouse.silver.now", side_effect=lambda: clock),
            patch("meteo_warehouse.bronze.time.sleep", side_effect=sleep) as sleeper,
            patch("meteo_warehouse.bronze.shutil.which", return_value="/usr/bin/curl"),
            patch("meteo_warehouse.bronze.subprocess.run", side_effect=curl) as curl_run,
        ):
            self.assertEqual(
                self.invoke("work", "--max-requests", "2", "--workers", "1"),
                [{"slice_id": slice_id, "status": "complete"}],
            )
        sleeper.assert_called_once_with(1)
        self.assertEqual(handler.call_count, 1)
        self.assertEqual(curl_run.call_count, 1)
        with duckdb.connect(str(self.database), read_only=True) as db:
            attempts = db.execute(
                "SELECT attempt_no, http_status, fetch_error FROM bronze.weather_forecast ORDER BY attempt_no"
            ).fetchall()
            self.assertEqual(attempts, [(1, None, "ConnectTimeout: TLS handshake timed out"), (2, 200, None)])
            self.assertEqual(db.execute("SELECT count(*) FROM ops.call_reservation").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 72)
        self.assertEqual(self.invoke("refresh", "forecast")["forecast"]["rows"], 24)

    def test_worker_can_resume_exhausted_transient_failure_without_duplicate_facts(self):
        slice_id = self.plan_forecast()
        client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(503)))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            failed = self.invoke("work", "--max-attempts", "1", expected=2)
        self.assertEqual(failed[0]["attempts"], 1)
        self.assertIn("--retry-failed", failed[0]["retry_hint"])
        self.assertEqual(self.plan_forecast(), slice_id)
        handler = Mock(return_value=httpx.Response(200, content=self.response_bytes()))
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            self.assertEqual(
                self.invoke("work", "--retry-failed", "--max-attempts", "2"),
                [{"slice_id": slice_id, "status": "complete"}],
            )
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            self.assertEqual(self.invoke("work", "--retry-failed", "--max-attempts", "2"), [])
        self.assertEqual(handler.call_count, 1)
        with duckdb.connect(str(self.database), read_only=True) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM bronze.weather_forecast").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 72)

    def test_retry_failed_skips_permanent_http_errors(self):
        self.plan_forecast()
        handler = Mock(return_value=httpx.Response(400))
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            self.invoke("work", "--max-attempts", "1", expected=2)
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
            self.assertEqual(self.invoke("work", "--retry-failed", "--max-attempts", "6"), [])
        self.assertEqual(handler.call_count, 1)

    def test_retry_output_and_wait_budget_explain_missing_forecast(self):
        self.plan_forecast()
        for arguments, failure in [
            (["--no-wait"], httpx.ConnectTimeout("DNS lookup timed out")),
            ([], httpx.Response(429, headers={"Retry-After": "120"})),
        ]:
            with self.subTest(arguments=arguments):
                with duckdb.connect(str(self.database)) as db:
                    db.execute("UPDATE ops.extraction_slice SET status='queued', not_before_at=NULL")
                handler = Mock(side_effect=[failure])
                client = httpx.Client(transport=httpx.MockTransport(handler))
                with (
                    patch("meteo_warehouse.cli.httpx.Client", return_value=client),
                    patch("meteo_warehouse.bronze.time.sleep") as sleeper,
                ):
                    result = self.invoke("work", "--http-backend", "httpx", *arguments, expected=2)
                self.assertEqual(result[0]["status"], "retryable")
                self.assertIsNotNone(result[0]["not_before_at"])
                sleeper.assert_not_called()
                self.assertEqual(handler.call_count, 1)
        self.assertEqual(self.invoke("refresh", "forecast", expected=2)["forecast"]["status"], "ineligible")

    def test_curl_preserves_http_failures_headers_and_limits_response_size(self):
        with httpx.Client() as client:
            for returncode, status, body, error in [
                (0, b"503", b"busy", None),
                (28, b"000", b"", "CurlError(28)"),
                (63, b"200", b"123456789", "response_too_large"),
            ]:
                with self.subTest(returncode=returncode):

                    def curl(command, response_body=body, exit_code=returncode, status_text=status, **kwargs):
                        Path(command[command.index("--output") + 1]).write_bytes(response_body)
                        Path(command[command.index("--dump-header") + 1]).write_text(
                            "HTTP/1.1 200 Connection established\r\nContent-Type: obsolete\r\n\r\n"
                            "HTTP/1.1 503 Unavailable\r\nRetry-After: 120\r\nContent-Type: text/plain\r\n\r\n"
                        )
                        return subprocess.CompletedProcess(command, exit_code, stdout=status_text, stderr=b"timed out")

                    with (
                        patch("meteo_warehouse.bronze.subprocess.run", side_effect=curl),
                        patch("meteo_warehouse.bronze.BODY_LIMIT", 8),
                    ):
                        response = fetch_curl(client, "https://api.open-meteo.com/v1/forecast", {"latitude": 52.52})
                    self.assertEqual(response.retry_after, "120")
                    self.assertEqual(response.content_type, "text/plain")
                    self.assertEqual(response.status, int(status) or None)
                    self.assertLessEqual(len(response.body), 8)
                    if error:
                        self.assertTrue(response.error.startswith(error))
                    else:
                        self.assertIsNone(response.error)

    def test_curl_process_errors_are_recorded_as_fetch_failures(self):
        with httpx.Client() as client:
            for failure in [FileNotFoundError("curl missing"), subprocess.TimeoutExpired("curl", 10)]:
                with self.subTest(failure=type(failure).__name__):
                    with patch("meteo_warehouse.bronze.subprocess.run", side_effect=failure):
                        response = fetch_curl(client, "https://api.open-meteo.com/v1/forecast", {})
                    self.assertTrue(response.error.startswith("CurlError:"))
                    self.assertIsNone(response.status)
                    self.assertIsNone(response.body)

    def test_model_poll_schedules_once_and_handles_invalid_metadata(self):
        metadata = {
            "last_run_initialisation_time": int(START.timestamp()),
            "last_run_availability_time": int((START + timedelta(minutes=10)).timestamp()),
            "temporal_resolution_seconds": 3600,
        }
        real_client = httpx.Client
        for payload, expected, status in [
            (metadata, 0, "planned"),
            (metadata, 0, "already_planned"),
            ({}, 2, "invalid"),
        ]:
            with self.subTest(status=status):
                client = real_client(
                    transport=httpx.MockTransport(lambda _, payload=payload: httpx.Response(200, json=payload))
                )
                with patch("meteo_warehouse.cli.httpx.Client", return_value=client):
                    self.assertEqual(self.invoke("poll", "single_runs", expected=expected)["status"], status)

    def test_benchmark_missing_database_reports_error(self):
        self.assertIn("weather-warehouse:", self.invoke("benchmark", expected=1))
        self.assertFalse(self.database.exists())
