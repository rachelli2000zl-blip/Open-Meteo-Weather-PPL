import csv
import json
import shutil
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import httpx

from weather_pipeline.bronze import fetch_capture, json_bytes, plan_capture, read_json, sha256, writer_lock
from weather_pipeline.cli import main
from weather_pipeline.publish import build, render_report


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "berlin"


class PipelineTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fixture = Path(shutil.copytree(FIXTURE, self.root / "bronze"))
        self.output = self.root / "output"
        self.output.mkdir()

    def response_path(self, product):
        manifest = read_json(self.fixture / "capture.json")
        request = next(request for request in manifest["requests"] if request["product"] == product)
        return self.fixture / "requests" / request["request_id"] / "001" / "response.json"

    def edit_response(self, product, edit):
        response_path = self.response_path(product)
        body = read_json(response_path)
        edit(body)
        new_body = json_bytes(body)
        response_path.write_bytes(new_body)
        meta_path = response_path.with_name("meta.json")
        meta = read_json(meta_path)
        meta.update(sha256=sha256(new_body), body_bytes=len(new_body))
        meta_path.write_bytes(json_bytes(meta))

    def replay(self):
        return build(self.fixture, self.output, recorded=True)

    def test_real_fixture_replays_with_stable_build_and_keys(self):
        summary, result = self.replay()
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["counts"], {"weather_samples": 504, "weather_daily": 14,
                                             "coverage": 23, "quality_issues": 0})
        self.assertEqual(self.replay(), (summary, result))
        self.assertIn("Recorded snapshot", (result / "outlook.html").read_text())
        with duckdb.connect() as conn:
            distinct = conn.execute("""
                SELECT count(DISTINCT (capture_id, product, site_id, model_id, section,
                                       valid_at_utc, variable_id, series_key)) FROM read_parquet(?)
            """, [str(result / "weather_samples.parquet")]).fetchone()[0]
        self.assertEqual(distinct, 504)

    def test_malformed_optional_series_preserves_valid_sibling(self):
        self.edit_response("archive", lambda body: body["daily"]["temperature_2m_min"].pop())
        summary, result = self.replay()
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["counts"]["weather_daily"], 7)
        self.assertIn("array_mismatch", (result / "quality_issues.csv").read_text())
        self.assertTrue((result / "outlook.html").exists())

    def test_required_missing_slot_blocks_even_with_same_count(self):
        self.replay()
        latest = (self.output / "latest.json").read_bytes()

        def replace_first_slot(body):
            axis = body["hourly"]["time"]
            axis.pop(0)
            axis.append(axis[-1] + 3600)

        self.edit_response("forecast", replace_first_slot)
        code = main(["replay", "--fixture", str(self.fixture), "--output", str(self.output)])
        summary, result = self.replay()
        self.assertEqual(code, 2)
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(len(summary["missing_forecast_hours"]), 1)
        self.assertEqual((self.output / "latest.json").read_bytes(), latest)
        self.assertFalse((result / "outlook.html").exists())

    def test_missing_ensemble_values_leave_band_gap_or_block(self):
        def drop_control(body):
            body["hourly"]["temperature_2m"][0] = None

        self.edit_response("ensemble", drop_control)
        summary, result = self.replay()
        self.assertEqual(summary["status"], "partial")
        with (result / "outlook_hourly.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual((rows[0]["valid_members"], rows[0]["p10"], rows[0]["p90"]), ("19", "", ""))
        self.assertNotEqual(rows[1]["p10"], "")

        def drop_all(body):
            for name, values in body["hourly"].items():
                if name != "time":
                    values[0] = None

        self.edit_response("ensemble", drop_all)
        summary, _ = self.replay()
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(len(summary["empty_ensemble_hours"]), 1)

    def test_duplicate_slots_collapse_or_quarantine(self):
        def add_duplicate(body):
            for values in body["hourly"].values():
                values.insert(0, values[0])

        self.edit_response("forecast", add_duplicate)
        summary, result = self.replay()
        self.assertEqual(summary["counts"]["weather_samples"], 504)
        self.assertIn("duplicate_slots", (result / "quality_issues.csv").read_text())

        def change_duplicate(body):
            body["hourly"]["temperature_2m"][0] += 10

        self.edit_response("forecast", change_duplicate)
        summary, result = self.replay()
        self.assertEqual(summary["status"], "blocked")
        self.assertIn("conflicting_duplicate", (result / "quality_issues.csv").read_text())

    def test_corrupt_bronze_is_detected_and_quarantined(self):
        raw = self.response_path("forecast")
        raw.write_bytes(raw.read_bytes() + b" ")
        summary, result = self.replay()
        self.assertEqual(summary["status"], "blocked")
        self.assertIn("checksum", (result / "quality_issues.csv").read_text())

    def test_writer_lock_and_interrupted_report_keep_latest(self):
        self.replay()
        latest = (self.output / "latest.json").read_bytes()
        self.edit_response("archive", lambda body: body["daily"]["temperature_2m_min"].pop())
        with patch("weather_pipeline.publish.render_report", side_effect=OSError("disk write failed")):
            with self.assertRaises(OSError):
                self.replay()
        self.assertEqual((self.output / "latest.json").read_bytes(), latest)
        with writer_lock(self.output), self.assertRaises(ValueError), writer_lock(self.output):
            self.fail("A second writer acquired the lock")

    def test_http_retry_lands_both_attempts_and_resume_reuses_success(self):
        directory, manifest = plan_capture(self.root)
        responses = [httpx.Response(429, headers={"Retry-After": "0"}, content=b"rate limited")]
        responses += [httpx.Response(200, content=b"{}") for _ in range(3)]
        transport = httpx.MockTransport(lambda request: responses.pop(0))
        for _ in range(2):
            client = httpx.Client(transport=transport)
            with patch("weather_pipeline.bronze.httpx.Client", return_value=client):
                fetch_capture(directory, manifest)
        self.assertEqual(responses, [])
        first = directory / "requests" / manifest["requests"][0]["request_id"] / "001"
        self.assertEqual((first / "response.json").read_bytes(), b"rate limited")
        self.assertEqual(read_json(first / "meta.json")["http_status"], 429)
        self.assertEqual(read_json(first.with_name("002") / "meta.json")["http_status"], 200)

    def test_berlin_repeated_clock_hour_displays_both_offsets(self):
        capture = read_json(self.fixture / "capture.json")
        hourly = [(datetime(2026, 10, 25, hour, tzinfo=UTC), 12 + hour, 11, 13,
                   20, 20, "request", "response.json") for hour in range(2)]
        report = render_report(capture, "passed", hourly, [], recorded=True)
        self.assertIn("25 Oct 02:00 CEST", report)
        self.assertIn("25 Oct 02:00 CET", report)
