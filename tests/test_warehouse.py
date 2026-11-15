import json
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import duckdb
import httpx

from meteo_warehouse.bronze import Response, land, poll, work
from meteo_warehouse.db import initialize, insert, now, uuid_key
from meteo_warehouse.gold import publish, refresh
from meteo_warehouse.operations import benchmark, snapshot
from meteo_warehouse.plan import get_slice, load_config, plan_job, replay_mappings
from meteo_warehouse.silver import normalize

HOUR = timedelta(hours=1)
BASE = datetime(2026, 4, 22, 6, tzinfo=UTC)
TARGET = BASE + 6 * HOUR


def hourly(start, fields, count=2):
    default_units = {"temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h",
                     "wind_gusts_10m": "m/s"}
    return {"latitude": 52.49, "longitude": 13.42, "elevation": 35, "timezone": "GMT",
            "utc_offset_seconds": 0, "hourly": dict(fields, time=[int((start + i * HOUR).timestamp())
                                                                for i in range(count)]),
            "hourly_units": {name: default_units.get(name, "°C") for name in fields}}


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
        slice_id, = plan_job(self.db, self.config, job, start=start, end=end, **kwargs)
        return get_slice(self.db, slice_id)

    def capture(self, slice_, payload, fetched=BASE, loaded=None, checksum=None, evidence=None, status=200):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        fetch_id = land(self.db, slice_, Response(status, "application/json", body, None, fetched),
                        requested_at=fetched, expected_sha256=checksum, evidence=evidence)
        with patch("meteo_warehouse.silver.now", return_value=loaded or fetched):
            result = normalize(self.db, slice_, fetch_id)
        return fetch_id, result

    def test_partial_shape_zero_null_and_verified_checksum(self):
        slice_ = self.plan("forecast")
        data = hourly(TARGET, {"temperature_2m": [68, 32], "precipitation": [0.0, None], "wind_speed_10m": [5]})
        data["hourly_units"]["temperature_2m"] = "°F"
        fetch, status = self.capture(slice_, data)
        self.assertEqual(status, "partial")
        self.assertEqual(self.db.execute("""
            SELECT value_number, quality_status FROM gold.series_values
            WHERE measure_code = 'precipitation' ORDER BY valid_at
        """).fetchall(), [(0.0, "valid"), (None, "source_null")])
        self.assertEqual(self.db.execute("""
            SELECT value_number, source_unit, site_latitude, source_latitude FROM gold.series_values
            WHERE measure_code = 'temperature_2m' ORDER BY valid_at
        """).fetchall(), [(20.0, "°F", 52.52, 52.49), (0.0, "°F", 52.52, 52.49)])
        self.assertEqual(self.db.execute("""
            SELECT rule_code FROM ops.quality_issue WHERE api_field = 'wind_speed_10m' ORDER BY rule_code
        """).fetchall(), [("array_length_mismatch",), ("incomplete_slots",)])
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
        self.assertEqual(self.db.execute("""
            SELECT value_number, quality_status, revision_no FROM gold.series_values
            WHERE measure_code = 'temperature_2m' AND valid_at = ? ORDER BY revision_no
        """, [TARGET]).fetchall(), [(10.0, "valid", 1), (12.0, "valid", 2), (10.0, "flagged", 3)])

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
        self.assertEqual(self.db.execute("SELECT DISTINCT release_id FROM gold.forecast_hourly_latest").fetchone()[0],
                         initial["release_id"])

    def test_ensemble_gap_and_idempotence(self):
        slice_ = self.plan("ensemble")
        fields = {"temperature_2m": [0, 0], **{f"temperature_2m_member{i:02}": [i, i] for i in range(1, 20)}}
        fields["temperature_2m_member19"][1] = None
        self.capture(slice_, hourly(TARGET, fields))
        kwargs = (self.db, self.config, "ensemble", TARGET, TARGET + 2 * HOUR)
        first = publish(*kwargs)
        self.assertEqual(publish(*kwargs)["release_id"], first["release_id"])
        self.assertEqual(self.db.execute("""
            SELECT expected_members, usable_members, p50, coverage_status
            FROM gold.ensemble_hourly ORDER BY valid_at
        """).fetchall(), [(20, 20, 9.5, "complete"), (20, 19, None, "partial")])

    def test_daily_dst_sums_full_preceding_windows(self):
        zone = ZoneInfo("Europe/Berlin")
        for day, following, expected in (("2025-03-30", "2025-03-31", 23), ("2025-10-26", "2025-10-27", 25)):
            start = datetime.fromisoformat(day).replace(tzinfo=zone).astimezone(UTC)
            end = datetime.fromisoformat(following).replace(tzinfo=zone).astimezone(UTC)
            slice_ = self.plan("archive", start, end + HOUR)
            n = expected + 1
            data = hourly(start, {"temperature_2m": [5] * n, "precipitation": [999, 0] + [1] * (n - 2),
                                  "wind_gusts_10m": [4] * n}, n)
            self.capture(slice_, data)
            publish(self.db, self.config, "weather_daily", day, following)
            row = self.db.execute("""
                SELECT epoch(window_end - window_start) / 3600, precip_mm, metric_coverage
                FROM gold.weather_daily WHERE local_date = ?
            """, [day]).fetchone()
            self.assertEqual(row[:2], (expected, expected - 1.0))
            self.assertEqual(json.loads(row[-1])["precip_mm"]["expected"], expected)

    def test_fixed_offset_provider_daily_keeps_source_window(self):
        slice_ = self.plan("archive_daily", "2025-07-21", "2025-07-22")
        data = {"latitude": 52.5, "longitude": 13.5, "timezone": "Europe/Berlin", "utc_offset_seconds": 3600,
                "daily": {"time": ["2025-07-21"], "temperature_2m_min": [9], "temperature_2m_max": [14]},
                "daily_units": {"temperature_2m_min": "°C", "temperature_2m_max": "°C"}}
        self.assertEqual(self.capture(slice_, data)[1], "complete")
        self.assertEqual(self.db.execute("""
            SELECT DISTINCT date_part('hour', window_start), source_utc_offset_seconds FROM silver.series_value
        """).fetchone(), (23, 3600))
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
        expected = {"silver": [(None, None), (10.0, 6.0), (10.0, 6.0)],
                    "gold": [(None, None), (None, None), (10.0, 6.0)]}
        for dataset, values in expected.items():
            self.assertEqual(self.db.execute("""
                SELECT forecast_temp_c, lead_hours FROM gold.ml_forecast_features
                WHERE dataset_id = ? AND valid_at = ? ORDER BY decision_at
            """, [dataset, TARGET]).fetchall(), values)
        publish(*gold_params, dataset="verified", basis="verified_source", **cutoffs)
        self.assertTrue(self.db.execute("""
            SELECT bool_and(missing_required_features) FROM gold.ml_forecast_features WHERE dataset_id = 'verified'
        """).fetchone()[0])
        labels["hourly"]["temperature_2m"][0] = 20
        correction = label_time + HOUR
        self.capture(label_slice, labels, fetched=correction, evidence=(correction, "label revision", True))
        self.assertEqual(publish(*gold_params, dataset="silver", **cutoffs)["build_id"], first["build_id"])
        publish(*gold_params, dataset="new", decisions=times, cutoff=correction)
        self.assertDictEqual(dict(self.db.execute("""
            SELECT dataset_id, target_temp_c FROM gold.ml_weather_labels
            WHERE valid_at = ? AND dataset_id IN ('silver', 'new')
        """, [TARGET]).fetchall()), {"silver": 12.0, "new": 20.0})

    def test_multi_site_order_and_refresh(self):
        self.config["jobs"]["forecast"]["sites"] = ["berlin", "oslo"]
        forecast_slice = self.plan("forecast")
        berlin = hourly(TARGET, forecast_sample(10)) | {"location_id": 0}
        oslo = hourly(TARGET, forecast_sample(5)) | {"location_id": 1, "latitude": 59.89, "longitude": 10.74}
        self.assertEqual(self.capture(forecast_slice, [oslo, berlin])[1], "complete")
        points = self.db.execute("""
            SELECT p.location_key, p.result_index, p.request_index, f.value_number
            FROM bronze.weather_forecast_point p JOIN gold.series_values f USING (fetch_id, result_index)
            WHERE f.measure_code = 'temperature_2m' AND f.valid_at = ? ORDER BY p.location_key
        """, [TARGET]).fetchall()
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
        markup = {"last_run_initialisation_time": int(BASE.timestamp()),
                  "last_run_availability_time": int((BASE + HOUR).timestamp()),
                  "temporal_resolution_seconds": 3600}
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
        job["field_overrides"] = {"temperature_2m": {"versions": [
            {"effective_to": BASE.isoformat(), "support": "unsupported"},
            {"effective_from": BASE.isoformat(), "horizon_hours": 2,
             "horizon_reference": "requested_window", "request_limits": {"max_locations": 1, "max_days": 1}},
        ]}}
        inactive = plan_job(self.db, self.config, "forecast", anchor=BASE-HOUR, start=TARGET, end=TARGET+2*HOUR)
        self.assertTrue(all(get_slice(self.db, sid)["fields"]["temperature_2m"]["support"] == "unsupported"
                            for sid in inactive))
        ids = plan_job(self.db, self.config, "forecast", anchor=BASE, start=TARGET, end=TARGET+25*HOUR)
        self.assertEqual(len(ids), 4)
        self.assertEqual(len({self.db.execute("SELECT capability_id FROM ops.slice_capability WHERE slice_id = ?",
                                              [slice_id]).fetchone()[0] for slice_id in ids}), 1)
        first = get_slice(self.db, ids[0])
        self.assertEqual(len(first["sites"]), 1)
        self.assertEqual(self.capture(first, hourly(TARGET, {"temperature_2m": [9, 10]}))[1], "complete")
        self.assertEqual(self.db.execute("""
            SELECT coverage_status, expected_slots FROM silver.coverage WHERE slice_id = ? ORDER BY window_start
        """, [first["id"]]).fetchall(), [("complete", 2), ("outside_horizon", 0)])

    def test_daily_provider_falls_back_on_dst(self):
        self.config["contracts"]["weather_daily"].update(reporting_start="2025-03-29", reporting_end="2025-03-31")
        zone = ZoneInfo("Europe/Berlin")
        start = datetime(2025, 3, 29, tzinfo=zone).astimezone(UTC)
        end = datetime(2025, 3, 31, tzinfo=zone).astimezone(UTC)
        n = int((end - start) / HOUR) + 1
        hourly_slice = self.plan("archive", start, end + HOUR)
        self.capture(hourly_slice, hourly(start, {"temperature_2m": [7] * n, "precipitation": [1] * n,
                                                "wind_gusts_10m": [2] * n}, n))
        provider_slice = self.plan("archive_daily", "2025-03-29", "2025-03-31")
        provider = {"latitude": 52.5, "longitude": 13.5, "timezone": "Europe/Berlin", "utc_offset_seconds": 3600,
                    "daily": {"time": ["2025-03-29", "2025-03-30"], "temperature_2m_min": [3, 3],
                              "temperature_2m_max": [17, 17]},
                    "daily_units": {"temperature_2m_min": "°C", "temperature_2m_max": "°C"}}
        self.capture(provider_slice, provider)
        self.assertEqual(refresh(self.db, self.config, ["weather_daily"])["weather_daily"][0]["rows"], 2)
        self.assertEqual(self.db.execute("""
            SELECT temp_min_c, precip_mm, json_extract_string(metric_coverage, '$.temp_min_c.source')
            FROM gold.weather_daily ORDER BY local_date
        """).fetchall(), [(3.0, 24.0, "provider_daily"), (7.0, 23.0, "hourly")])

    def test_rolling_inputs_use_revision_cutoffs_on_both_surfaces(self):
        slice_ = self.plan("single_runs", TARGET-2*HOUR, TARGET+HOUR, run=BASE)
        initial = BASE + timedelta(minutes=12)
        later = BASE + timedelta(minutes=20)
        fields = forecast_sample(count=3)
        fields["precipitation"] = [1, None, 4]
        self.capture(slice_, hourly(TARGET-2*HOUR, fields, 3), fetched=initial)
        release = self.config["contracts"]["single_run_forecast"]
        release.update(hours=3, required=["temp_2m_c"])
        with patch("meteo_warehouse.gold.now", return_value=initial):
            early = publish(self.db, self.config, "single_run_forecast", TARGET-2*HOUR, TARGET+HOUR)
        fields["precipitation"][1] = 2
        self.capture(slice_, hourly(TARGET-2*HOUR, fields, 3), fetched=later,
                     evidence=(later, "test provider revision", True))
        with patch("meteo_warehouse.gold.now", return_value=later):
            late = publish(self.db, self.config, "single_run_forecast", TARGET-2*HOUR, TARGET+HOUR)
        self.assertNotEqual(early["release_id"], late["release_id"])
        self.config["contracts"]["training"].update(job="single_runs", release_contract="single_run_forecast")
        decisions = (initial + timedelta(minutes=1), later + timedelta(minutes=1))
        for surface in ("silver", "gold"):
            publish(self.db, self.config, "training", TARGET, TARGET+HOUR, decisions=decisions,
                    cutoff=decisions[-1], dataset=surface, surface=surface)
            result = self.db.execute("""
                SELECT forecast_precip_mm, json_extract(rolling_features, '$.forecast_precip_3h_mm'),
                       json_extract(missing_features, '$.forecast_precip_3h_mm')
                FROM gold.ml_forecast_features WHERE dataset_id = ? ORDER BY decision_at
            """, [surface]).fetchall()
            self.assertEqual(result, [(4.0, "null", "true"), (4.0, "7.0", "false")])

    def test_snapshot_independent_reader_and_benchmark(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            with initialize(path / "live.duckdb") as writer:
                first = snapshot(writer, path / "snapshots")
                with duckdb.connect(first["path"], read_only=True) as reader:
                    plan_job(writer, self.config, "forecast", start=TARGET, end=TARGET+2*HOUR)
                    self.assertEqual(reader.execute("SELECT count(*) FROM ops.extraction_slice").fetchone()[0], 0)
                    second = snapshot(writer, path / "snapshots")
                    self.assertNotEqual(first["version"], second["version"])
                    self.assertEqual(json.loads((path / "snapshots/latest.json").read_text())["version"],
                                     second["version"])
                    self.assertEqual(benchmark(reader, self.config, trials=2)["query_latency"]["daily_report"],
                                     {"status": "no_data", "rows": 0})


if __name__ == "__main__":
    unittest.main()
