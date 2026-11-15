import json
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from .bronze import CONFIG, MAX_BODY_BYTES, attempts, read_json, sha256


@dataclass
class Quality:
    samples: list = field(default_factory=list)
    daily: list = field(default_factory=list)
    coverage: list = field(default_factory=list)
    issues: list = field(default_factory=list)

    def issue(self, request, raw_path, code, detail, source_field=None, slot=None):
        self.issues.append((request["request_id"], request["product"], request["section"],
                            raw_path, code, detail, source_field, slot))


def capture_slots(capture):
    window = capture["window"]
    start = datetime.fromisoformat(window["forecast_start_utc"])
    stop = datetime.fromisoformat(window["forecast_stop_utc"])
    if (start.utcoffset() != timedelta(0) or stop - start != timedelta(hours=CONFIG["forecast_hours"])
            or start != start.replace(minute=0, second=0, microsecond=0)):
        raise ValueError("Capture manifest must define an on-the-hour 24-hour UTC window")
    hours = [int((start + timedelta(hours=i)).timestamp()) for i in range(CONFIG["forecast_hours"])]
    first, last = [date.fromisoformat(window[key]) for key in ("archive_start", "archive_end")]
    if (last - first).days != 6:
        raise ValueError("Capture manifest must define seven archive dates")
    dates = [(first + timedelta(days=i)).isoformat() for i in range(7)]
    return hours, dates


def read_capture(directory):
    capture = read_json(directory / "capture.json")
    if (capture["schema"] != 1 or capture["site_id"] != CONFIG["site_id"]
            or capture["timezone"] != CONFIG["timezone"]
            or not re.fullmatch(r"[a-zA-Z0-9_-]+", capture["capture_id"])
            or [request["product"] for request in capture["requests"]]
            != ["forecast", "ensemble", "archive"]):
        raise ValueError("Invalid Berlin capture manifest")
    for request in capture["requests"]:
        if (not re.fullmatch(r"[a-f0-9]{20}", request["request_id"])
                or request["model"] != CONFIG[request["product"]]["model"]
                or request["section"] != CONFIG[request["product"]]["section"]
                or request["fields"] != CONFIG[request["product"]]["fields"]):
            raise ValueError("Capture request does not match the Berlin v1 profile")
    return capture


def read_responses(directory, capture, quality):
    responses, fingerprint = {}, []
    for request in capture["requests"]:
        committed = attempts(directory, request)
        last_failed = None
        if not committed:
            quality.issue(request, None, "request_unavailable", "No committed HTTP attempt")
        for attempt in committed:
            raw_file = attempt / "response.json"
            path = raw_file.relative_to(directory).as_posix()
            try:
                meta = read_json(attempt / "meta.json")
                if raw_file.exists() and raw_file.stat().st_size > MAX_BODY_BYTES:
                    raise ValueError("Stored response exceeds the byte limit")
                raw = raw_file.read_bytes() if raw_file.exists() else None
                digest = sha256(raw) if raw is not None else None
                fingerprint.append((path, meta, digest))
                if (digest != meta["sha256"] or (len(raw) if raw is not None else 0) != meta["body_bytes"]
                        or (meta["complete"] and raw is None)):
                    raise ValueError("Stored body does not match its checksum and size")
            except (OSError, KeyError, TypeError, ValueError) as exc:
                fingerprint.append((path, "invalid_attempt"))
                quality.issue(request, path, "invalid_attempt", str(exc))
                continue
            if meta["complete"] and meta["http_status"] == 200:
                responses[request["request_id"]] = (path, raw)
            else:
                detail = meta.get("error") or f"HTTP {meta.get('http_status')}"
                last_failed = path if raw is not None else None, detail
        if committed and request["request_id"] not in responses:
            path, detail = last_failed or (None, "No verified HTTP 200 response")
            quality.issue(request, path, "request_failed", detail)
    return responses, fingerprint


def unique_pairs(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"Duplicate JSON key {name!r}")
        result[name] = value
    return result


def invalid_constant(name):
    raise ValueError(f"Non-JSON numeric literal {name}")


def numeric(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def expected_series(request):
    if request["product"] == "ensemble":
        # The unsuffixed field is member 0 in the pinned ICON-D2-EPS JSON contract.
        name = request["fields"][0]
        return [(name, name, "member00")] + [
            (f"{name}_member{i:02}", name, f"member{i:02}")
            for i in range(1, CONFIG["ensemble"]["members"])
        ]
    return [(name, name, "single") for name in request["fields"]]


def transform(capture, responses, hours, dates, quality):
    for request in capture["requests"]:
        expected = hours if request["section"] == "hourly" else dates
        series = expected_series(request)
        response = responses.get(request["request_id"])
        if response is None:
            for source, variable, key in series:
                add_coverage(quality, request, source, variable, key, len(expected), "request_failed")
            continue
        path, raw = response
        section = request["section"]
        try:
            body = json.loads(raw, object_pairs_hook=unique_pairs, parse_constant=invalid_constant)
            if not isinstance(body, dict) or body.get("error"):
                raise ValueError("API error or invalid top-level JSON shape")
            data, units = body.get(section), body.get(f"{section}_units")
            if not isinstance(data, dict) or not isinstance(units, dict):
                raise ValueError(f"Missing or invalid {section} data or units object")
            if section == "hourly" and body.get("utc_offset_seconds") != 0:
                raise ValueError("Hourly response does not declare a zero UTC offset")
            if section == "daily" and body.get("timezone") != capture["timezone"]:
                raise ValueError("Daily response timezone differs from the Berlin request")
            times = data.get("time")
            if not valid_axis(times, units.get("time"), section):
                raise ValueError(f"Invalid {section} time axis or time unit")
        except (ValueError, RecursionError) as exc:
            quality.issue(request, path, "invalid_section", str(exc))
            for source, variable, key in series:
                add_coverage(quality, request, source, variable, key, len(expected), "invalid_section")
            continue

        unknown = set(data) - {source for source, _, _ in series} - {"time"}
        for name in sorted(unknown):
            quality.issue(request, path, "unknown_field", "Field has no v1 mapping", name)
        target = set(expected)
        outside = set(times) - target
        if outside:
            quality.issue(request, path, "unexpected_slots", f"{len(outside)} outside the target window")
        for source, variable, key in series:
            normalize_series(capture, request, path, data, units, times, target, source, variable,
                             key, quality)


def valid_axis(times, unit, section):
    if not isinstance(times, list):
        return False
    if section == "hourly":
        valid = unit == "unixtime" and all(type(slot) is int and slot % 3600 == 0 for slot in times)
    else:
        if unit != "iso8601":
            return False
        try:
            valid = all(isinstance(slot, str) and date.fromisoformat(slot).isoformat() == slot
                        for slot in times)
        except ValueError:
            return False
    return valid and all(a <= b for a, b in zip(times, times[1:]))


def add_coverage(quality, request, source, variable, key, expected, status,
                 returned=0, valid=0, nulls=0, invalid=0, duplicates=0):
    quality.coverage.append((request["request_id"], request["product"], request["model"],
                             request["section"], variable, key, source, expected, returned, valid,
                             nulls, invalid, expected - returned, duplicates, status))


def normalize_series(capture, request, path, data, units, times, target, source, variable, key, quality):
    observed = len(set(times) & target)
    status = None
    if source not in data:
        status = "missing_field"
    elif not isinstance(data[source], list) or len(data[source]) != len(times):
        status = "array_mismatch"
    elif units.get(source) != "°C":
        status = "unexpected_unit"
    if status:
        quality.issue(request, path, status, "Expected an aligned Celsius series", source)
        add_coverage(quality, request, source, variable, key, len(target), status,
                     returned=0 if status == "missing_field" else observed,
                     invalid=0 if status == "missing_field" else observed)
        return

    values, rejected = {}, set()
    duplicates = 0
    for slot, value in zip(times, data[source], strict=True):
        if slot not in target:
            continue
        if value is not None and not numeric(value):
            quality.issue(request, path, "invalid_value", "Expected a finite number or null", source, str(slot))
            rejected.add(slot)
        elif slot in values:
            duplicates += 1
            if values[slot] != value:
                rejected.add(slot)
                quality.issue(request, path, "conflicting_duplicate", "Values disagree at same key", source, str(slot))
        else:
            values[slot] = value
    if duplicates:
        quality.issue(request, path, "duplicate_slots", f"{duplicates} repeated in-window values", source)
    if observed < len(target):
        missing = sorted(target - set(times))
        quality.issue(request, path, "missing_slots", ", ".join(map(str, missing)), source)

    valid, nulls = 0, 0
    for slot, value in values.items():
        if slot in rejected:
            continue
        valid += value is not None
        nulls += value is None
        row = (capture["capture_id"], capture["site_id"], request["product"], request["model"],
               request["section"])
        if request["section"] == "hourly":
            row += (datetime.fromtimestamp(slot, UTC),)
            destination = quality.samples
        else:
            row += (date.fromisoformat(slot), capture["timezone"])
            destination = quality.daily
        destination.append(row + (variable, key, source, "°C", value, request["request_id"], path))

    status = "populated"
    if not valid and nulls and not rejected and observed == len(target):
        status = "all_null"
    elif valid != len(target) or duplicates:
        status = "partial"
    add_coverage(quality, request, source, variable, key, len(target), status, observed,
                 valid, nulls, len(rejected), duplicates)
