import json
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import duckdb
import httpx

from meteo_warehouse.bronze import Response, land, poll, work
from meteo_warehouse.contract_fingerprint import LEGACY_SOURCE_SHA256, implementation_fingerprint
from meteo_warehouse.db import initialize, insert, insert_many, now, uuid_key
from meteo_warehouse.gold import gold_contract, publish, refresh
from meteo_warehouse.operations import benchmark, snapshot
from meteo_warehouse.plan import get_slice, load_config, plan_job, replay_mappings
from meteo_warehouse.silver import normalize

HOUR = timedelta(hours=1)
BASE = datetime(2026, 4, 22, 6, tzinfo=UTC)
TARGET = BASE + 6 * HOUR


def hourly(start, fields, count=2):
    default_units = {"temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h", "wind_gusts_10m": "m/s"}
    return {
        "latitude": 52.49,
        "longitude": 13.42,
        "elevation": 35,
        "timezone": "GMT",
        "utc_offset_seconds": 0,
        "hourly": dict(fields, time=[int((start + i * HOUR).timestamp()) for i in range(count)]),
        "hourly_units": {name: default_units.get(name, "°C") for name in fields},
    }


def forecast_sample(temp=10, count=2):
    return {"temperature_2m": [temp] * count, "precipitation": [0] * count, "wind_speed_10m": [18] * count}


class WarehouseTest(unittest.TestCase):
    def setUp(self):
        self.db = initialize(":memory:")
        self.addCleanup(self.db.close)
        self.config = load_config()
        for name in ("archive", "archive_daily"):
            self.config["jobs"][name]["sites"] = ["berlin"]
        for name in ("forecast", "ensemble", "single_run_forecast"):
            self.config["contracts"][name]["hours"] = 2

    def plan(self, job, start=TARGET, end=TARGET + 2 * HOUR, **kwargs):
        (slice_id,) = plan_job(self.db, self.config, job, start=start, end=end, **kwargs)
        return get_slice(self.db, slice_id)

    def capture(self, slice_, payload, fetched=BASE, loaded=None, checksum=None, evidence=None, status=200):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        fetch_id = land(
            self.db,
            slice_,
            Response(status, "application/json", body, None, fetched),
            requested_at=fetched,
            expected_sha256=checksum,
            evidence=evidence,
        )
        with patch("meteo_warehouse.silver.now", return_value=loaded or fetched):
            result = normalize(self.db, slice_, fetch_id)
        return fetch_id, result

    def test_planner_rejects_unaligned_subdaily_windows_before_writing(self):
        for job, start, end in [
            ("forecast", TARGET + timedelta(minutes=30), TARGET + 24 * HOUR + timedelta(minutes=30)),
            ("forecast", TARGET + timedelta(seconds=1), TARGET + HOUR + timedelta(seconds=1)),
            ("forecast", TARGET + timedelta(microseconds=1), TARGET + HOUR + timedelta(microseconds=1)),
            ("forecast", TARGET, TARGET + HOUR + timedelta(minutes=30)),
            ("minutely", TARGET + timedelta(minutes=5), TARGET + HOUR + timedelta(minutes=5)),
        ]:
            with self.subTest(job=job, start=start, end=end):
                with self.assertRaisesRegex(ValueError, "align"):
                    plan_job(self.db, self.config, job, start=start, end=end)
                for table in ("ops.extraction_slice", "silver.site", "silver.coverage", "ops.api_capability"):
                    self.assertEqual(self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)

    def test_planner_accepts_hourly_and_minutely_cadences(self):
        self.assertTrue(
            plan_job(
                self.db, self.config, "forecast", start="2026-04-22T12:00:00+02:00", end="2026-04-22T14:00:00+02:00"
            )
        )
        self.assertTrue(
            plan_job(self.db, self.config, "minutely", start=TARGET + timedelta(minutes=15), end=TARGET + HOUR)
        )

    def test_batch_insert_rejects_unequal_row_lengths_before_writing(self):
        self.db.execute("CREATE TEMP TABLE shape_check (a INTEGER, b INTEGER)")
        with self.assertRaises(ValueError):
            insert_many(self.db, "shape_check", [(1, 2), (3,)])
        self.assertEqual(self.db.execute("SELECT count(*) FROM shape_check").fetchone()[0], 0)

    def test_gold_contract_preserves_legacy_records_and_checks_implementation_changes(self):
        source = Path(__file__).resolve().parents[1] / "src/meteo_warehouse/gold.py"
        original = source.read_text(encoding="utf-8")
        expected = gold_contract(self.db, self.config, "forecast")
        for stored in (LEGACY_SOURCE_SHA256, implementation_fingerprint(original)):
            with self.subTest(stored=stored), TemporaryDirectory() as directory:
                self.db.execute("UPDATE ops.gold_contract SET sql_sha256 = ?", [stored])
                variant = Path(directory) / "gold.py"
                cleaned = "# Formatting-only edit\n" + original.replace(
                    "from datetime import date, timedelta", "from datetime import UTC, date, datetime, timedelta"
                )
                variant.write_text(cleaned, encoding="utf-8")
                with patch("meteo_warehouse.gold.PACKAGE", Path(directory)):
                    self.assertEqual(gold_contract(self.db, self.config, "forecast"), expected)
                    self.assertEqual(self.db.execute("SELECT sql_sha256 FROM ops.gold_contract").fetchone()[0], stored)
                    changed = cleaned.replace("INTERVAL '1 hour'", "INTERVAL '2 hours'", 1)
                    self.assertNotEqual(changed, cleaned)
                    variant.write_text(changed, encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "Bump contract"):
                        gold_contract(self.db, self.config, "forecast")
                    next_config = {
                        **self.config,
                        "contracts": {
                            **self.config["contracts"],
                            "forecast": {**self.config["contracts"]["forecast"], "version": "test-next"},
                        },
                    }
                    gold_contract(self.db, next_config, "forecast")
                self.db.execute("DELETE FROM ops.gold_contract WHERE contract_version = 'test-next'")

    def test_gold_contract_rejects_unknown_hash_and_changed_rules(self):
        gold_contract(self.db, self.config, "forecast")
        self.db.execute("UPDATE ops.gold_contract SET sql_sha256 = 'unknown-legacy-hash'")
        with self.assertRaisesRegex(ValueError, "Bump contract"):
            gold_contract(self.db, self.config, "forecast")
        self.db.execute("UPDATE ops.gold_contract SET sql_sha256 = ?", [LEGACY_SOURCE_SHA256])
        self.config["contracts"]["forecast"]["hours"] = 3
        with self.assertRaisesRegex(ValueError, "Bump contract"):
            gold_contract(self.db, self.config, "forecast")

    def test_worker_fetches_normalizes_and_publishes(self):
        slice_ = self.plan("forecast")
        payload = hourly(TARGET, forecast_sample())
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
            self.assertEqual(work(self.db, client), [(slice_["id"], "complete")])
        self.assertEqual(refresh(self.db, self.config, ["forecast"])["forecast"]["rows"], 2)
        self.assertEqual(
            self.db.execute("SELECT temp_2m_c FROM gold.forecast_hourly_latest ORDER BY valid_at").fetchall(),
            [(10.0,), (10.0,)],
        )

    def test_historical_validation_pins_forecasts_and_checks_latest_matching_reference(self):
        forecast = self.plan("single_runs", run=BASE)
        values = forecast_sample()
        values["temperature_2m"] = [10, 12]
        self.capture(forecast, hourly(TARGET, values))
        release = publish(self.db, self.config, "single_run_forecast", TARGET, TARGET + 2 * HOUR)
        query = """
            SELECT forecast_temp_c, reference_temp_c, error_c, reference_status, reference_revision_no
            FROM gold.forecast_temperature_validation ORDER BY valid_at
        """
        self.assertEqual(
            self.db.execute(query).fetchall(), [(10, None, None, "missing", None), (12, None, None, "missing", None)]
        )

        self.config["jobs"]["validation_reference"]["params"] = {"cell_selection": "nearest"}
        other_profile = self.plan("validation_reference")
        self.capture(other_profile, hourly(TARGET, {"temperature_2m": [40, 40]}))
        self.assertEqual(
            self.db.execute(query).fetchall(), [(10, None, None, "missing", None), (12, None, None, "missing", None)]
        )
        del self.config["jobs"]["validation_reference"]["params"]

        reference = self.plan("validation_reference")
        self.capture(reference, hourly(TARGET, {"temperature_2m": [9, 10]}), fetched=BASE + HOUR)
        self.assertEqual(self.db.execute(query).fetchall(), [(10, 9, 1, "complete", 1), (12, 10, 2, "complete", 1)])
        self.assertEqual(
            self.db.execute("""
            SELECT count(error_c), avg(absolute_error_c), avg(error_c * error_c), avg(error_c)
            FROM gold.forecast_temperature_validation
        """).fetchone(),
            (2, 1.5, 2.5, 1.5),
        )

        values["temperature_2m"] = [20, 22]
        self.capture(
            forecast,
            hourly(TARGET, values),
            fetched=BASE + 2 * HOUR,
            evidence=(BASE + 2 * HOUR, "test forecast correction", True),
        )
        self.assertEqual(self.db.execute(query).fetchall(), [(10, 9, 1, "complete", 1), (12, 10, 2, "complete", 1)])
        self.capture(
            reference,
            hourly(TARGET, {"temperature_2m": [None, 11]}),
            fetched=BASE + 3 * HOUR,
            evidence=(BASE + 3 * HOUR, "test reference correction", True),
        )
        self.assertEqual(
            self.db.execute(query).fetchall(), [(10, None, None, "unusable", 2), (12, 11, 1, "complete", 2)]
        )

        updated = publish(self.db, self.config, "single_run_forecast", TARGET, TARGET + 2 * HOUR)
        self.assertNotEqual(updated["release_id"], release["release_id"])
        self.assertEqual(
            self.db.execute(query).fetchall(), [(20, None, None, "unusable", 2), (22, 11, 11, "complete", 2)]
        )
        self.assertEqual(
            self.db.execute("SELECT DISTINCT release_id FROM gold.forecast_temperature_validation").fetchall(),
            [(updated["release_id"],)],
        )

    def test_worker_waits_before_retrying_transient_failures(self):
        failures = [
            ("seconds", httpx.Response(503, headers={"Retry-After": "7"}), 7),
            (
                "http_date",
                httpx.Response(
                    429, headers={"Retry-After": format_datetime(BASE + timedelta(seconds=11), usegmt=True)}
                ),
                11,
            ),
            ("invalid_header", httpx.Response(503, headers={"Retry-After": "invalid"}), 1),
            ("timeout", httpx.ReadTimeout("Connection interrupted"), 1),
        ]
        for index, (name, failure, delay) in enumerate(failures):
            with self.subTest(failure=name):
                start = TARGET + index * 2 * HOUR
                slice_ = self.plan("forecast", start, start + 2 * HOUR)
                handler = Mock(side_effect=[failure, httpx.Response(200, json=hourly(start, forecast_sample()))])
                eligible = BASE + timedelta(seconds=delay)
                with (
                    httpx.Client(transport=httpx.MockTransport(handler)) as client,
                    patch("meteo_warehouse.bronze.now", return_value=BASE) as clock,
                    patch("meteo_warehouse.silver.now", return_value=BASE),
                ):
                    self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [(slice_["id"], "failed")])
                    self.assertEqual(
                        self.db.execute(
                            "SELECT status, not_before_at FROM ops.extraction_slice WHERE slice_id = ?",
                            [slice_["id"]],
                        ).fetchone(),
                        ("retryable", eligible),
                    )
                    clock.return_value = eligible - timedelta(microseconds=1)
                    self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [])
                    self.assertEqual(handler.call_count, 1)
                    clock.return_value = eligible
                    self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [(slice_["id"], "complete")])
                    self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [])
                self.assertEqual(handler.call_count, 2)
                self.assertEqual(
                    self.db.execute(
                        "SELECT attempt_no FROM bronze.weather_forecast WHERE slice_id = ? ORDER BY attempt_no",
                        [slice_["id"]],
                    ).fetchall(),
                    [(1,), (2,)],
                )
                self.assertEqual(
                    self.db.execute(
                        """
                        SELECT count(*) FROM silver.series_value f
                        JOIN bronze.weather_forecast b USING (fetch_id) WHERE b.slice_id = ?
                        """,
                        [slice_["id"]],
                    ).fetchone()[0],
                    6,
                )

    def test_worker_stops_after_retry_limit(self):
        slice_ = self.plan("forecast")
        handler = Mock(return_value=httpx.Response(503))
        with (
            httpx.Client(transport=httpx.MockTransport(handler)) as client,
            patch("meteo_warehouse.bronze.now", return_value=BASE) as clock,
        ):
            self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [(slice_["id"], "failed")])
            clock.return_value = BASE + timedelta(seconds=1)
            self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [(slice_["id"], "failed")])
            clock.return_value = BASE + HOUR
            self.assertEqual(work(self.db, client, max_attempts=2, workers=1), [])
        self.assertEqual(handler.call_count, 2)
        self.assertEqual(get_slice(self.db, slice_["id"])["status"], "failed")
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.weather_forecast").fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 0)

    def test_worker_resumes_latest_landed_attempt_after_restart(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "recovery.duckdb"
            handler = Mock(
                side_effect=[httpx.Response(503), httpx.Response(200, json=hourly(TARGET, forecast_sample()))]
            )
            with initialize(path) as writer:
                (slice_id,) = plan_job(writer, self.config, "forecast", start=TARGET, end=TARGET + 2 * HOUR)
                with (
                    httpx.Client(transport=httpx.MockTransport(handler)) as client,
                    patch("meteo_warehouse.bronze.now", return_value=BASE) as clock,
                ):
                    self.assertEqual(work(writer, client, max_attempts=2, workers=1), [(slice_id, "failed")])
                    clock.return_value = BASE + timedelta(seconds=1)
                    with patch(
                        "meteo_warehouse.silver.normalize", side_effect=RuntimeError("Interrupted after landing")
                    ):
                        with self.assertRaisesRegex(RuntimeError, "Interrupted after landing"):
                            work(writer, client, max_attempts=2, workers=1)
                self.assertEqual(get_slice(writer, slice_id)["status"], "landed")
                self.assertEqual(writer.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 0)
                fetch_id = writer.execute(
                    "SELECT fetch_id FROM bronze.weather_forecast WHERE attempt_no = 2"
                ).fetchone()[0]
            self.assertEqual(handler.call_count, 2)

            unexpected_http = Mock(side_effect=AssertionError("Recovery must reuse landed bytes"))
            with initialize(path) as writer, httpx.Client(transport=httpx.MockTransport(unexpected_http)) as client:
                self.assertEqual(work(writer, client, max_attempts=2, workers=1), [(slice_id, "complete")])
                self.assertEqual(work(writer, client, max_attempts=2, workers=1), [])
                self.assertEqual(normalize(writer, get_slice(writer, slice_id), fetch_id), "complete")
                self.assertEqual(writer.execute("SELECT count(*) FROM bronze.weather_forecast").fetchone()[0], 2)
                self.assertEqual(writer.execute("SELECT count(*) FROM ops.call_reservation").fetchone()[0], 2)
                self.assertEqual(writer.execute("SELECT count(*) FROM silver.series_value_source").fetchone()[0], 6)
                self.assertEqual(
                    writer.execute("SELECT DISTINCT fetch_id FROM silver.series_value").fetchall(), [(fetch_id,)]
                )
                self.assertEqual(writer.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 6)
                self.assertEqual(refresh(writer, self.config, ["forecast"])["forecast"]["status"], "published")
            unexpected_http.assert_not_called()

    def test_partial_shape_zero_null_and_verified_checksum(self):
        slice_ = self.plan("forecast")
        data = hourly(TARGET, {"temperature_2m": [68, 32], "precipitation": [0.0, None], "wind_speed_10m": [5]})
        data["hourly_units"]["temperature_2m"] = "°F"
        fetch, status = self.capture(slice_, data)
        self.assertEqual(status, "partial")
        self.assertEqual(
            self.db.execute("""
            SELECT value_number, quality_status FROM gold.series_values
            WHERE measure_code = 'precipitation' ORDER BY valid_at
        """).fetchall(),
            [(0.0, "valid"), (None, "source_null")],
        )
        self.assertEqual(
            self.db.execute("""
            SELECT value_number, source_unit, site_latitude, source_latitude FROM gold.series_values
            WHERE measure_code = 'temperature_2m' ORDER BY valid_at
        """).fetchall(),
            [(20.0, "°F", 52.52, 52.49), (0.0, "°F", 52.52, 52.49)],
        )
        self.assertEqual(
            self.db.execute("""
            SELECT rule_code FROM ops.quality_issue WHERE api_field = 'wind_speed_10m' ORDER BY rule_code
        """).fetchall(),
            [("array_length_mismatch",), ("incomplete_slots",)],
        )
        self.assertEqual(normalize(self.db, slice_, fetch), "partial")
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 4)
        self.assertEqual(self.capture(slice_, b"{}", checksum="0" * 64)[1], "invalid")
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.weather_forecast").fetchone()[0], 2)

    def test_archive_duplicate_and_mapping_revision(self):
        slice_ = self.plan("archive")
        data = hourly(TARGET, {"temperature_2m": [10, 10], "precipitation": [0, 0], "wind_gusts_10m": [3, 3]})
        first, _ = self.capture(slice_, data)
        self.capture(slice_, data, fetched=BASE + HOUR)
        data["hourly"]["temperature_2m"] = [12, 10]
        _, partial = self.capture(slice_, data, fetched=BASE + 2 * HOUR)
        self.assertEqual(partial, "partial")
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 6)
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.series_value_source").fetchone()[0], 17)
        corrected = BASE + 3 * HOUR
        self.capture(slice_, data, fetched=corrected, evidence=(corrected, "provider revision record", True))
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.series_value").fetchone()[0], 7)
        self.config["fields"]["temperature_2m"]["mapping_version"] = 2
        self.config["fields"]["temperature_2m"]["plausible_max"] = 9
        versioned = replay_mappings(slice_, self.config, "archive")
        normalize(self.db, versioned, first)
        self.assertEqual(
            self.db.execute(
                """
            SELECT value_number, quality_status, revision_no FROM gold.series_values
            WHERE measure_code = 'temperature_2m' AND valid_at = ? ORDER BY revision_no
        """,
                [TARGET],
            ).fetchall(),
            [(10.0, "valid", 1), (12.0, "valid", 2), (10.0, "flagged", 3)],
        )

    def test_live_bundles_cannot_create_one_release_and_failed_refresh_retains_previous(self):
        slice_ = self.plan("forecast")
        self.capture(slice_, hourly(TARGET, forecast_sample()), fetched=BASE + HOUR)
        initial = publish(self.db, self.config, "forecast", TARGET, TARGET + 2 * HOUR)
        self.assertEqual(initial["status"], "published")
        self.config["jobs"]["forecast"]["batch_fields"] = 1
        later = TARGET + HOUR
        for slice_id in plan_job(self.db, self.config, "forecast", start=later, end=later + 2 * HOUR):
            bundle = get_slice(self.db, slice_id)
            payload = {field: forecast_sample()[field] for field in bundle["fields"]}
            self.capture(bundle, hourly(later, payload), fetched=BASE + 2 * HOUR)
        rejected = publish(self.db, self.config, "forecast", later, later + 2 * HOUR)
        self.assertEqual(rejected["status"], "ineligible")
        self.assertEqual(
            self.db.execute("SELECT DISTINCT release_id FROM gold.forecast_hourly_latest").fetchone()[0],
            initial["release_id"],
        )

    def test_ensemble_gap_and_idempotence(self):
        slice_ = self.plan("ensemble")
        fields = {"temperature_2m": [0, 0], **{f"temperature_2m_member{i:02}": [i, i] for i in range(1, 20)}}
        fields["temperature_2m_member19"][1] = None
        self.capture(slice_, hourly(TARGET, fields))
        kwargs = (self.db, self.config, "ensemble", TARGET, TARGET + 2 * HOUR)
        first = publish(*kwargs)
        self.assertEqual(publish(*kwargs)["release_id"], first["release_id"])
        self.assertEqual(
            self.db.execute("""
            SELECT expected_members, usable_members, p50, coverage_status
            FROM gold.ensemble_hourly ORDER BY valid_at
        """).fetchall(),
            [(20, 20, 9.5, "complete"), (20, 19, None, "partial")],
        )

    def test_daily_dst_sums_full_preceding_windows(self):
        zone = ZoneInfo("Europe/Berlin")
        for day, following, expected in (("2025-03-30", "2025-03-31", 23), ("2025-10-26", "2025-10-27", 25)):
            start = datetime.fromisoformat(day).replace(tzinfo=zone).astimezone(UTC)
            end = datetime.fromisoformat(following).replace(tzinfo=zone).astimezone(UTC)
            slice_ = self.plan("archive", start, end + HOUR)
            n = expected + 1
            data = hourly(
                start,
                {"temperature_2m": [5] * n, "precipitation": [999, 0] + [1] * (n - 2), "wind_gusts_10m": [4] * n},
                n,
            )
            self.capture(slice_, data)
            publish(self.db, self.config, "weather_daily", day, following)
            row = self.db.execute(
                """
                SELECT epoch(window_end - window_start) / 3600, precip_mm, metric_coverage
                FROM gold.weather_daily WHERE local_date = ?
            """,
                [day],
            ).fetchone()
            self.assertEqual(row[:2], (expected, expected - 1.0))
            self.assertEqual(json.loads(row[-1])["precip_mm"]["expected"], expected)

    def test_fixed_offset_provider_daily_keeps_source_window(self):
        slice_ = self.plan("archive_daily", "2025-07-21", "2025-07-22")
        data = {
            "latitude": 52.5,
            "longitude": 13.5,
            "timezone": "Europe/Berlin",
            "utc_offset_seconds": 3600,
            "daily": {"time": ["2025-07-21"], "temperature_2m_min": [9], "temperature_2m_max": [14]},
            "daily_units": {"temperature_2m_min": "°C", "temperature_2m_max": "°C"},
        }
        self.assertEqual(self.capture(slice_, data)[1], "complete")
        self.assertEqual(
            self.db.execute("""
            SELECT DISTINCT date_part('hour', window_start), source_utc_offset_seconds FROM silver.series_value
        """).fetchone(),
            (23, 3600),
        )
        publish(self.db, self.config, "weather_provider_daily", "2025-07-21", "2025-07-22")
        self.assertEqual(self.db.execute("SELECT temp_min_c FROM gold.weather_daily").fetchone(), (None,))

    def test_training_cutoffs_and_frozen_label(self):
        slice_ = self.plan("single_runs", run=BASE)
        loaded = BASE + timedelta(minutes=12)
        published = BASE + timedelta(minutes=14)
        self.capture(slice_, hourly(TARGET, forecast_sample()), fetched=loaded, loaded=loaded)
        with patch("meteo_warehouse.gold.now", return_value=published):
            publish(self.db, self.config, "single_run_forecast", TARGET, TARGET + 2 * HOUR)
        config = self.config
        config["contracts"]["training"].update(job="single_runs", release_contract="single_run_forecast")
        label_slice = self.plan("archive")
        labels = hourly(TARGET, {"temperature_2m": [12, 13], "precipitation": [0, 0], "wind_gusts_10m": [3, 3]})
        label_time = TARGET + 24 * HOUR
        self.capture(label_slice, labels, fetched=label_time)
        times = [BASE + timedelta(minutes=i) for i in (10, 13, 14)]
        cutoffs = dict(decisions=times, cutoff=label_time)
        gold_params = (self.db, config, "training", TARGET, TARGET + 2 * HOUR)
        first = publish(*gold_params, dataset="silver", **cutoffs)
        publish(*gold_params, dataset="gold", surface="gold", **cutoffs)
        expected = {
            "silver": [(None, None), (10.0, 6.0), (10.0, 6.0)],
            "gold": [(None, None), (None, None), (10.0, 6.0)],
        }
        for dataset, values in expected.items():
            self.assertEqual(
                self.db.execute(
                    """
                SELECT forecast_temp_c, lead_hours FROM gold.ml_forecast_features
                WHERE dataset_id = ? AND valid_at = ? ORDER BY decision_at
            """,
                    [dataset, TARGET],
                ).fetchall(),
                values,
            )
        publish(*gold_params, dataset="verified", basis="verified_source", **cutoffs)
        self.assertTrue(
            self.db.execute("""
            SELECT bool_and(missing_required_features) FROM gold.ml_forecast_features WHERE dataset_id = 'verified'
        """).fetchone()[0]
        )
        labels["hourly"]["temperature_2m"][0] = 20
        correction = label_time + HOUR
        self.capture(label_slice, labels, fetched=correction, evidence=(correction, "label revision", True))
        self.assertEqual(publish(*gold_params, dataset="silver", **cutoffs)["build_id"], first["build_id"])
        publish(*gold_params, dataset="new", decisions=times, cutoff=correction)
        self.assertDictEqual(
            dict(
                self.db.execute(
                    """
            SELECT dataset_id, target_temp_c FROM gold.ml_weather_labels
            WHERE valid_at = ? AND dataset_id IN ('silver', 'new')
        """,
                    [TARGET],
                ).fetchall()
            ),
            {"silver": 12.0, "new": 20.0},
        )

    def test_multi_site_order_and_refresh(self):
        self.config["jobs"]["forecast"]["sites"] = ["berlin", "oslo"]
        forecast_slice = self.plan("forecast")
        berlin = hourly(TARGET, forecast_sample(10)) | {"location_id": 0}
        oslo = hourly(TARGET, forecast_sample(5)) | {"location_id": 1, "latitude": 59.89, "longitude": 10.74}
        self.assertEqual(self.capture(forecast_slice, [oslo, berlin])[1], "complete")
        points = self.db.execute(
            """
            SELECT p.location_key, p.result_index, p.request_index, f.value_number
            FROM bronze.weather_forecast_point p JOIN gold.series_values f USING (fetch_id, result_index)
            WHERE f.measure_code = 'temperature_2m' AND f.valid_at = ? ORDER BY p.location_key
        """,
            [TARGET],
        ).fetchall()
        self.assertEqual(points, [("berlin", 1, 0, 10.0), ("oslo", 0, 1, 5.0)])
        self.assertEqual(refresh(self.db, self.config, ["forecast"])["forecast"]["rows"], 2)
        self.assertEqual(refresh(self.db, self.config, ["forecast"])["forecast"]["status"], "unchanged")

    def test_quota_and_polls(self):
        plan = self.plan("forecast")
        for units in (600,):
            insert(self.db, "ops.call_reservation", [uuid_key(units), now(), Decimal(units), False])
        with httpx.Client(transport=httpx.MockTransport(lambda _: self.fail("Unexpected HTTP request"))) as client:
            self.assertEqual(work(self.db, client), [(plan["id"], "deferred_quota")])
        self.db.execute("DELETE FROM ops.call_reservation")
        markup = {
            "last_run_initialisation_time": int(BASE.timestamp()),
            "last_run_availability_time": int((BASE + HOUR).timestamp()),
            "temporal_resolution_seconds": 3600,
        }
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=markup))) as client:
            result, ids = poll(self.db, client, self.config, "single_runs")
            self.assertEqual(result, "planned")
            self.assertEqual(poll(self.db, client, self.config, "single_runs"), ("already_planned", ids))
        result = self.db.execute("""
            SELECT e.not_before_at, e.requested_run_at, o.source_available_at
            FROM ops.extraction_slice e JOIN silver.model_update_observation o ON e.trigger_poll_id = o.poll_id
        """).fetchone()
        self.assertEqual(result, (BASE + timedelta(minutes=70), BASE, BASE + HOUR))

    def test_capability_versions_limits_and_live_horizon(self):
        job = self.config["jobs"]["forecast"]
        job.update(fields=["temperature_2m"], sites=["berlin", "oslo"], batch_sites=2, batch_days=2)
        job["field_overrides"] = {
            "temperature_2m": {
                "versions": [
                    {"effective_to": BASE.isoformat(), "support": "unsupported"},
                    {
                        "effective_from": BASE.isoformat(),
                        "horizon_hours": 2,
                        "horizon_reference": "requested_window",
                        "request_limits": {"max_locations": 1, "max_days": 1},
                    },
                ]
            }
        }
        inactive = plan_job(self.db, self.config, "forecast", anchor=BASE - HOUR, start=TARGET, end=TARGET + 2 * HOUR)
        self.assertTrue(
            all(get_slice(self.db, sid)["fields"]["temperature_2m"]["support"] == "unsupported" for sid in inactive)
        )
        ids = plan_job(self.db, self.config, "forecast", anchor=BASE, start=TARGET, end=TARGET + 25 * HOUR)
        self.assertEqual(len(ids), 4)
        self.assertEqual(
            len(
                {
                    self.db.execute(
                        "SELECT capability_id FROM ops.slice_capability WHERE slice_id = ?", [slice_id]
                    ).fetchone()[0]
                    for slice_id in ids
                }
            ),
            1,
        )
        first = get_slice(self.db, ids[0])
        self.assertEqual(len(first["sites"]), 1)
        self.assertEqual(self.capture(first, hourly(TARGET, {"temperature_2m": [9, 10]}))[1], "complete")
        self.assertEqual(
            self.db.execute(
                """
            SELECT coverage_status, expected_slots FROM silver.coverage WHERE slice_id = ? ORDER BY window_start
        """,
                [first["id"]],
            ).fetchall(),
            [("complete", 2), ("outside_horizon", 0)],
        )

    def test_daily_provider_falls_back_on_dst(self):
        self.config["contracts"]["weather_daily"].update(reporting_start="2025-03-29", reporting_end="2025-03-31")
        zone = ZoneInfo("Europe/Berlin")
        start = datetime(2025, 3, 29, tzinfo=zone).astimezone(UTC)
        end = datetime(2025, 3, 31, tzinfo=zone).astimezone(UTC)
        n = int((end - start) / HOUR) + 1
        hourly_slice = self.plan("archive", start, end + HOUR)
        self.capture(
            hourly_slice,
            hourly(start, {"temperature_2m": [7] * n, "precipitation": [1] * n, "wind_gusts_10m": [2] * n}, n),
        )
        provider_slice = self.plan("archive_daily", "2025-03-29", "2025-03-31")
        provider = {
            "latitude": 52.5,
            "longitude": 13.5,
            "timezone": "Europe/Berlin",
            "utc_offset_seconds": 3600,
            "daily": {
                "time": ["2025-03-29", "2025-03-30"],
                "temperature_2m_min": [3, 3],
                "temperature_2m_max": [17, 17],
            },
            "daily_units": {"temperature_2m_min": "°C", "temperature_2m_max": "°C"},
        }
        self.capture(provider_slice, provider)
        self.assertEqual(refresh(self.db, self.config, ["weather_daily"])["weather_daily"][0]["rows"], 2)
        self.assertEqual(
            self.db.execute("""
            SELECT temp_min_c, precip_mm, json_extract_string(metric_coverage, '$.temp_min_c.source')
            FROM gold.weather_daily ORDER BY local_date
        """).fetchall(),
            [(3.0, 24.0, "provider_daily"), (7.0, 23.0, "hourly")],
        )

    def test_rolling_inputs_use_revision_cutoffs_on_both_surfaces(self):
        slice_ = self.plan("single_runs", TARGET - 2 * HOUR, TARGET + HOUR, run=BASE)
        initial = BASE + timedelta(minutes=12)
        later = BASE + timedelta(minutes=20)
        fields = forecast_sample(count=3)
        fields["precipitation"] = [1, None, 4]
        self.capture(slice_, hourly(TARGET - 2 * HOUR, fields, 3), fetched=initial)
        release = self.config["contracts"]["single_run_forecast"]
        release.update(hours=3, required=["temp_2m_c"])
        with patch("meteo_warehouse.gold.now", return_value=initial):
            early = publish(self.db, self.config, "single_run_forecast", TARGET - 2 * HOUR, TARGET + HOUR)
        fields["precipitation"][1] = 2
        self.capture(
            slice_,
            hourly(TARGET - 2 * HOUR, fields, 3),
            fetched=later,
            evidence=(later, "test provider revision", True),
        )
        with patch("meteo_warehouse.gold.now", return_value=later):
            late = publish(self.db, self.config, "single_run_forecast", TARGET - 2 * HOUR, TARGET + HOUR)
        self.assertNotEqual(early["release_id"], late["release_id"])
        self.config["contracts"]["training"].update(job="single_runs", release_contract="single_run_forecast")
        decisions = (initial + timedelta(minutes=1), later + timedelta(minutes=1))
        for surface in ("silver", "gold"):
            publish(
                self.db,
                self.config,
                "training",
                TARGET,
                TARGET + HOUR,
                decisions=decisions,
                cutoff=decisions[-1],
                dataset=surface,
                surface=surface,
            )
            result = self.db.execute(
                """
                SELECT forecast_precip_mm, json_extract(rolling_features, '$.forecast_precip_3h_mm'),
                       json_extract(missing_features, '$.forecast_precip_3h_mm')
                FROM gold.ml_forecast_features WHERE dataset_id = ? ORDER BY decision_at
            """,
                [surface],
            ).fetchall()
            self.assertEqual(result, [(4.0, "null", "true"), (4.0, "7.0", "false")])

    def test_geocoding_search_preserves_places_and_replay(self):
        slice_ = self.plan("geocoding")
        places = [
            {"id": 2950159, "name": "Berlin", "latitude": 52.52, "longitude": 13.405, "country_code": "DE"},
            {"id": 1000001, "name": "Berlin Mitte", "latitude": 52.53, "longitude": 13.39, "country_code": "DE"},
        ]
        fetch_id, status = self.capture(slice_, {"results": places})
        self.assertEqual(status, "complete")
        self.assertEqual(normalize(self.db, slice_, fetch_id), "complete")
        self.assertEqual(
            self.db.execute(
                """
                SELECT result_index, place_id, result_json_path
                FROM bronze.geocoding_result WHERE fetch_id = ? ORDER BY result_index
                """,
                [fetch_id],
            ).fetchall(),
            [(0, 2950159, "$.results[0]"), (1, 1000001, "$.results[1]")],
        )
        self.assertEqual(
            self.db.execute(
                """
                SELECT m.result_index, p.place_id, p.latitude, p.longitude, n.language, n.place_name
                FROM silver.geocode_match m JOIN silver.place p USING (place_id)
                JOIN silver.place_name n USING (place_id) WHERE m.fetch_id = ? ORDER BY m.result_index
                """,
                [fetch_id],
            ).fetchall(),
            [(0, 2950159, 52.52, 13.405, "en", "Berlin"), (1, 1000001, 52.53, 13.39, "en", "Berlin Mitte")],
        )
        self.assertEqual(
            self.db.execute("SELECT coverage_status, returned_slots, usable_slots FROM silver.coverage").fetchall(),
            [("complete", 2, 2)],
        )
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.geocoding").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM ops.quality_issue").fetchone()[0], 0)

    def test_geocoding_id_lookup_requires_requested_place(self):
        self.config["jobs"]["geocoding"]["params"] = {"id": 2950159, "language": "de"}
        slice_ = self.plan("geocoding")
        self.assertTrue(slice_["product"]["url"].endswith("/v1/get"))
        place = {"id": 2950159, "name": "Berlin", "latitude": 52.52, "longitude": 13.405}
        for payload, expected_rows, issue in [
            (place, 1, None),
            (place | {"id": 3143244}, 0, "invalid_geocode_place"),
            ({}, 0, "invalid_geocoding_response"),
        ]:
            with self.subTest(payload=payload):
                fetch_id, status = self.capture(slice_, payload)
                self.assertEqual(status, "complete" if expected_rows else "partial")
                self.assertEqual(
                    self.db.execute(
                        "SELECT place_id, requested_language, result_json_path FROM bronze.geocoding_result WHERE fetch_id = ?",
                        [fetch_id],
                    ).fetchall(),
                    [(2950159, "de", "$")] if expected_rows else [],
                )
                self.assertEqual(
                    self.db.execute(
                        "SELECT rule_code FROM ops.quality_issue WHERE fetch_id = ?", [fetch_id]
                    ).fetchall(),
                    [(issue,)] if issue else [],
                )
        self.assertEqual(self.db.execute("SELECT place_id FROM silver.place").fetchall(), [(2950159,)])
        self.assertEqual(
            self.db.execute("SELECT language, place_name FROM silver.place_name").fetchall(), [("de", "Berlin")]
        )

    def test_geocoding_distinguishes_empty_search_from_malformed_response(self):
        slice_ = self.plan("geocoding")
        for payload, coverage, issue in [
            ({"results": []}, "empty_result", None),
            ({"generationtime_ms": 0.1}, "empty_result", None),
            ({"results": {}}, "invalid_response", "invalid_geocoding_response"),
            ({"unexpected": []}, "invalid_response", "invalid_geocoding_response"),
        ]:
            with self.subTest(payload=payload):
                fetch_id, status = self.capture(slice_, payload)
                self.assertEqual(status, "complete" if coverage == "empty_result" else "partial")
                self.assertEqual(
                    self.db.execute("SELECT coverage_status, usable_slots FROM silver.coverage").fetchall(),
                    [(coverage, 0)],
                )
                self.assertEqual(
                    self.db.execute(
                        "SELECT rule_code FROM ops.quality_issue WHERE fetch_id = ?", [fetch_id]
                    ).fetchall(),
                    [(issue,)] if issue else [],
                )
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.geocoding").fetchone()[0], 4)
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.geocoding_result").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.place").fetchone()[0], 0)

    def test_geocoding_rejects_invalid_places_and_preserves_valid_result(self):
        slice_ = self.plan("geocoding")
        place = {"id": 2950159, "name": "Berlin", "latitude": 52.52, "longitude": 13.405}
        payload = {"results": [place, place | {"id": True}, place | {"id": 1000001, "latitude": 91}, place]}
        fetch_id, status = self.capture(slice_, payload)
        self.assertEqual(status, "partial")
        self.assertEqual(
            self.db.execute("SELECT result_index, place_id FROM bronze.geocoding_result").fetchall(), [(0, 2950159)]
        )
        self.assertEqual(self.db.execute("SELECT place_id FROM silver.place").fetchall(), [(2950159,)])
        self.assertEqual(
            self.db.execute("SELECT result_index, place_id FROM silver.geocode_match").fetchall(), [(0, 2950159)]
        )
        self.assertEqual(
            self.db.execute(
                "SELECT api_field, rule_code FROM ops.quality_issue WHERE fetch_id = ? ORDER BY api_field", [fetch_id]
            ).fetchall(),
            [("1", "invalid_geocode_place"), ("2", "invalid_geocode_place"), ("3", "invalid_geocode_place")],
        )
        self.assertEqual(
            self.db.execute("SELECT returned_slots, usable_slots, invalid_slots FROM silver.coverage").fetchall(),
            [(4, 1, 3)],
        )

    def test_elevation_matches_requested_sites_and_replay(self):
        slice_ = self.plan("elevation")
        fetch_id, status = self.capture(slice_, {"elevation": [0, -5, 23.5]})
        self.assertEqual(status, "complete")
        self.assertEqual(normalize(self.db, slice_, fetch_id), "complete")
        self.assertEqual(
            self.db.execute(
                """
                SELECT s.location_key, e.request_index, e.elevation_m, b.requested_latitude,
                       b.requested_longitude, b.result_json_path
                FROM silver.elevation e JOIN silver.site s USING (site_id)
                JOIN bronze.elevation_result b USING (fetch_id, request_index)
                WHERE e.fetch_id = ? ORDER BY e.request_index
                """,
                [fetch_id],
            ).fetchall(),
            [
                ("berlin", 0, 0.0, 52.52, 13.405, "$.elevation[0]"),
                ("helgoland", 1, -5.0, 54.174, 7.89, "$.elevation[1]"),
                ("oslo", 2, 23.5, 59.9, 10.75, "$.elevation[2]"),
            ],
        )
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.elevation").fetchone()[0], 1)
        self.assertEqual(
            self.db.execute(
                "SELECT coverage_status, usable_slots FROM silver.coverage ORDER BY location_key"
            ).fetchall(),
            [("complete", 1)] * 3,
        )
        self.assertEqual(self.db.execute("SELECT count(*) FROM ops.quality_issue").fetchone()[0], 0)

    def test_elevation_keeps_valid_site_and_records_missing_or_invalid_values(self):
        slice_ = self.plan("elevation")
        fetch_id, status = self.capture(slice_, {"elevation": [35, None, "invalid"]})
        self.assertEqual(status, "partial")
        self.assertEqual(
            self.db.execute(
                "SELECT s.location_key, e.elevation_m FROM silver.elevation e JOIN silver.site s USING (site_id)"
            ).fetchall(),
            [("berlin", 35.0)],
        )
        self.assertEqual(self.db.execute("SELECT request_index FROM bronze.elevation_result").fetchall(), [(0,)])
        self.assertEqual(
            self.db.execute(
                "SELECT location_key, rule_code FROM ops.quality_issue WHERE fetch_id = ? ORDER BY location_key",
                [fetch_id],
            ).fetchall(),
            [("helgoland", "missing_elevation"), ("oslo", "invalid_elevation")],
        )
        self.assertEqual(
            self.db.execute("""
                SELECT location_key, coverage_status, usable_slots, explicit_null_slots, invalid_slots
                FROM silver.coverage ORDER BY location_key
            """).fetchall(),
            [
                ("berlin", "complete", 1, 0, 0),
                ("helgoland", "missing_elevation", 0, 1, 0),
                ("oslo", "partial", 0, 0, 1),
            ],
        )

    def test_elevation_rejects_ambiguous_or_missing_response_arrays(self):
        slice_ = self.plan("elevation")
        for payload, reason in [
            ({"elevation": []}, "missing_elevation"),
            ({"elevation": [35]}, "ambiguous_elevation"),
            ({"elevation": [35, 5, 23, 99]}, "ambiguous_elevation"),
            ({"elevation": "invalid"}, "invalid_elevation_response"),
            ({}, "invalid_elevation_response"),
        ]:
            with self.subTest(payload=payload):
                fetch_id, status = self.capture(slice_, payload)
                self.assertEqual(status, "partial")
                self.assertEqual(
                    self.db.execute("SELECT coverage_status, usable_slots FROM silver.coverage").fetchall(),
                    [(reason, 0)] * 3,
                )
                self.assertEqual(
                    self.db.execute(
                        "SELECT rule_code FROM ops.quality_issue WHERE fetch_id = ?", [fetch_id]
                    ).fetchall(),
                    [(reason,)],
                )
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.elevation").fetchone()[0], 5)
        self.assertEqual(self.db.execute("SELECT count(*) FROM bronze.elevation_result").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM silver.elevation").fetchone()[0], 0)

    def test_benchmark_exports_training_rows_and_values_to_parquet(self):
        loaded = BASE + timedelta(minutes=12)
        end = TARGET + 4 * HOUR
        forecast_slice = self.plan("single_runs", TARGET - 2 * HOUR, end, run=BASE)
        fields = {
            "temperature_2m": [7, 8, 9, 10, 11, 12],
            "precipitation": [1, 2, 3, 4, None, 6],
            "wind_speed_10m": [18] * 6,
        }
        self.assertEqual(
            self.capture(forecast_slice, hourly(TARGET - 2 * HOUR, fields, 6), fetched=loaded)[1], "partial"
        )
        label_time = TARGET + 24 * HOUR
        labels = {"temperature_2m": [12, 13, 14, None], "precipitation": [0] * 4, "wind_gusts_10m": [3] * 4}
        self.capture(self.plan("archive", TARGET, end), hourly(TARGET, labels, 4), fetched=label_time)
        self.config["contracts"]["training"].update(job="single_runs", release_contract="single_run_forecast")
        early = loaded - timedelta(minutes=1)
        decision = loaded + timedelta(minutes=1)
        for dataset in ("export-fixture", "other-fixture"):
            result = publish(
                self.db,
                self.config,
                "training",
                TARGET,
                end,
                dataset=dataset,
                decisions=[early, decision],
                cutoff=label_time,
            )
            self.assertEqual((result["status"], result["features"], result["labels"]), ("published", 8, 4))

        with TemporaryDirectory() as directory:
            with patch("meteo_warehouse.operations.TemporaryDirectory", return_value=nullcontext(directory)):
                result = benchmark(self.db, self.config, trials=2, dataset="export-fixture")
            training = result["query_latency"]["training_parquet_export"]
            self.assertEqual((training["status"], training["rows"], training["trials"]), ("measured", 2, 2))
            self.assertGreaterEqual(training["p95_ms"], 0)
            file = Path(directory) / "training.parquet"
            site_id = self.db.execute("SELECT site_id FROM silver.site WHERE location_key = 'berlin'").fetchone()[0]
            self.assertEqual(
                self.db.execute(
                    """
                    SELECT site_id, decision_at, valid_at, lead_hours, forecast_temp_c, forecast_precip_mm, target_temp_c
                    FROM read_parquet(?) ORDER BY valid_at
                    """,
                    [str(file)],
                ).fetchall(),
                [
                    (site_id, decision, TARGET, 6.0, 9.0, 3.0, 12.0),
                    (site_id, decision, TARGET + HOUR, 7.0, 10.0, 4.0, 13.0),
                ],
            )
            values = self.db.execute(
                "SELECT valid_at, horizon_hours, rolling_features, missing_features FROM read_parquet(?) ORDER BY valid_at",
                [str(file)],
            ).fetchall()
            for row, precipitation, temperature in zip(values, (6.0, 9.0), (8.0, 9.0), strict=True):
                valid_at, horizon, rolling, missing = row
                self.assertAlmostEqual(horizon, (valid_at - decision).total_seconds() / 3600)
                self.assertEqual(
                    json.loads(rolling),
                    {"forecast_precip_3h_mm": precipitation, "forecast_temp_mean_3h_c": temperature},
                )
                self.assertEqual(
                    json.loads(missing),
                    {
                        "forecast_temp_c": False,
                        "forecast_precip_mm": False,
                        "forecast_precip_3h_mm": False,
                        "forecast_temp_mean_3h_c": False,
                    },
                )
        self.assertEqual(self.db.execute("SELECT count(*) FROM gold.ml_forecast_features").fetchone()[0], 16)
        self.assertEqual(self.db.execute("SELECT count(*) FROM gold.ml_weather_labels").fetchone()[0], 8)

    def test_snapshot_independent_reader_and_benchmark(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            with initialize(path / "live.duckdb") as writer:
                first = snapshot(writer, path / "snapshots")
                with duckdb.connect(first["path"], read_only=True) as reader:
                    plan_job(writer, self.config, "forecast", start=TARGET, end=TARGET + 2 * HOUR)
                    self.assertEqual(reader.execute("SELECT count(*) FROM ops.extraction_slice").fetchone()[0], 0)
                    second = snapshot(writer, path / "snapshots")
                    self.assertNotEqual(first["version"], second["version"])
                    self.assertEqual(
                        json.loads((path / "snapshots/latest.json").read_text())["version"], second["version"]
                    )
                    self.assertEqual(
                        benchmark(reader, self.config, trials=2)["query_latency"]["daily_report"],
                        {"status": "no_data", "rows": 0},
                    )


if __name__ == "__main__":
    unittest.main()
