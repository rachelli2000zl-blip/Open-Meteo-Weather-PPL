import math
import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta, timezone
from itertools import pairwise
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .bronze import checksum_matches, read_attempt, strict_json
from .db import digest, insert_many, instant, json_text, key, now, transaction
from .plan import local_midnight, period_end, slots

VALUE_TYPES = ("number", "code", "timestamp", "boolean", "text")
PERIODS = {"daily", "weekly", "monthly"}


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def coordinate(obj, name, lower, upper):
    value = obj.get(name)
    if not finite(value) or not lower <= value <= upper:
        raise ValueError(f"Invalid {name}")
    return float(value)


def put(db, table, pk, values):
    db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in values)}) ON CONFLICT ({pk}) DO NOTHING", values)
    return values[0]


def anchor_profile(db, profile):
    options = profile["options"]
    elevation = options.get("elevation")
    mode = "disabled" if str(elevation).lower() == "nan" else "default" if elevation is None else "override"
    return put(
        db,
        "silver.request_profile",
        "profile_id",
        [
            key(["profile", profile]),
            options.get("cell_selection"),
            mode,
            elevation if mode == "override" else None,
            profile["timezone"],
            options.get("tilt"),
            options.get("azimuth"),
            json_text(options),
        ],
    )


def measure_definition(rule):
    return [
        rule["measure_code"],
        rule.get("definition_version", 1),
        rule.get("quantity", rule["measure_code"]),
        rule.get("vertical_kind", "surface"),
        rule.get("level_from"),
        rule.get("level_to"),
        rule.get("level_unit"),
        rule["statistic"],
        rule["unit"],
        rule.get("type", "number"),
    ]


def field_mapping(db, product_id, selector, block, field, rule):
    definition = measure_definition(rule)
    measure_id = key(["measure", definition])
    put(db, "silver.measure", "measure_id", [measure_id, *definition[2:], *definition[1::-1]])
    mapping_version = rule.get("mapping_version", 1)
    rule_key = [
        "statistic",
        "window",
        "window_seconds",
        "conversions",
        "min",
        "max",
        "plausible_min",
        "plausible_max",
        "type",
        "unit",
        "daily_time_basis",
        "units_key",
    ]
    semantics = {name: rule[name] for name in rule_key if name in rule}
    mapping_id = key(["mapping", product_id, selector, block, field, mapping_version, measure_id, semantics])
    put(
        db,
        "silver.field_mapping",
        "mapping_id",
        [
            mapping_id,
            product_id,
            selector,
            block,
            field,
            measure_id,
            rule.get("window", "instant"),
            mapping_version,
            json_text(semantics),
        ],
    )
    return mapping_id, measure_id, mapping_version


def record_issues(db, slice_, records):
    for fetch_id, location, block, field, start, end, code, detail in records:
        identity = digest([slice_["id"], fetch_id, location, block, field, start, end, code])
        put(
            db,
            "ops.quality_issue",
            "issue_key",
            [
                identity,
                slice_["id"],
                fetch_id,
                location,
                block,
                field,
                start,
                end,
                code,
                "flagged" if code == "unusual_value" else "quarantined",
                json_text(detail),
            ],
        )


def horizon_end(slice_, rule):
    hours = rule.get("horizon_hours")
    if hours is None or rule.get("support", "supported") != "supported":
        return slice_["end"]
    basis = rule.get("horizon_reference", "run")
    anchor = slice_.get("horizon_anchors", {}).get(basis)
    anchor = (
        instant(anchor)
        if anchor
        else (slice_.get("run") if basis == "run" else slice_["start"] if basis == "requested_window" else None)
    )
    if anchor is None:
        return slice_["end"]
    return min(slice_["end"], max(slice_["start"], anchor + timedelta(hours=hours)))


def expected_count(slice_, rule, start, end):
    return sum(1 for _ in slots(start, end, ZoneInfo(slice_["timezone"]), slice_["block"], slice_["step"]))


def coverage_status(rule, fallback):
    support = rule.get("support", "supported")
    return fallback if support == "supported" else support


def empty_row(slice_, location, field, rule, reason, fetch_id, *, start=None, end=None):
    start = slice_["start"] if start is None else start
    end = slice_["end"] if end is None else end
    count = expected_count(slice_, rule, start, end) if reason != "outside_horizon" else 0
    members = rule.get("members")
    known_grid = slice_["step"] is not None or slice_["block"] in PERIODS or reason == "outside_horizon"
    return [
        slice_["id"],
        location,
        slice_["block"],
        field,
        start,
        end,
        fetch_id,
        coverage_status(rule, reason),
        count if known_grid else None,
        0,
        0,
        0,
        0,
        count if known_grid else None,
        len(members) if members is not None else None,
        0 if members is not None else None,
    ]


def empty_coverage(slice_, location, field, rule, reason, fetch_id):
    upper = horizon_end(slice_, rule)
    if upper > slice_["start"]:
        yield empty_row(slice_, location, field, rule, reason, fetch_id, end=upper)
    if upper < slice_["end"]:
        yield empty_row(slice_, location, field, rule, "outside_horizon", fetch_id, start=upper)


def append_coverage(db, slice_, rows, status):
    db.execute("DELETE FROM silver.coverage WHERE slice_id = ?", [slice_["id"]])
    insert_many(db, "silver.coverage", rows)
    db.execute(
        "UPDATE ops.extraction_slice SET coverage_result = ?, status = ? WHERE slice_id = ?",
        [status, status, slice_["id"]],
    )


def unavailable(db, slice_, reason, fetch_id=None, detail=None):
    locations = slice_["sites"] or [{"location_key": "__query__"}]
    fields = slice_["fields"] or {slice_["category"]: {}}
    rows = [
        row
        for site in locations
        for field, rule in fields.items()
        for row in empty_coverage(slice_, site["location_key"], field, rule, reason, fetch_id)
    ]
    status = (
        "failed"
        if reason == "fetch_failed"
        else "invalid"
        if reason in {"checksum_mismatch", "invalid_response"}
        else reason
    )
    if reason not in {"unsupported", "unverified", "deferred_quota", "outside_horizon"}:
        record_issues(
            db, slice_, [(fetch_id, None, slice_["block"], None, slice_["start"], slice_["end"], reason, detail)]
        )
    with transaction(db):
        latest = None
        if fetch_id:
            table = f"bronze.{slice_['category']}"
            latest = db.execute(
                f"""
                SELECT fetch_id FROM {table} WHERE slice_id = ? ORDER BY attempt_no DESC LIMIT 1
            """,
                [slice_["id"]],
            ).fetchone()
        if latest is None or latest[0] == fetch_id:
            append_coverage(db, slice_, rows, status)
    return status


def source_time(raw, offset):
    zone = timezone(timedelta(seconds=offset))
    if type(raw) is int:
        return datetime.fromtimestamp(raw, zone)
    if not isinstance(raw, str):
        raise ValueError("Invalid timestamp")
    parsed = datetime.fromisoformat(raw)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=zone)


def normalized_axis(block, raw, offset):
    source = source_time(raw, offset)
    if block in PERIODS and any((source.hour, source.minute, source.second, source.microsecond)):
        raise ValueError("Calendar samples must start at the provider's midnight")
    return source.astimezone(UTC), source.date(), int(source.utcoffset().total_seconds())


def interval(rule, valid_at, day, offset, block, zone, step):
    window = rule.get("window", "instant")
    if window == "instant":
        return valid_at, valid_at
    if window == "calendar" and block in PERIODS:
        if rule.get("daily_time_basis", "fixed_offset") == "fixed_offset":
            zone = timezone(timedelta(seconds=offset))
        if valid_at != local_midnight(day, zone):
            raise ValueError("Period start disagrees with the configured calendar")
        return valid_at, local_midnight(period_end(day, block), zone)
    seconds = rule.get("window_seconds", step)
    if window == "preceding" and seconds:
        return valid_at - timedelta(seconds=seconds), valid_at
    if window == "following" and seconds:
        return valid_at, valid_at + timedelta(seconds=seconds)
    raise ValueError("Window rule lacks a verified interval")


def conversion(unit, rule):
    canonical = rule["unit"]
    if unit == canonical:
        return 1, 0
    configured = rule.get("conversions", {}).get(unit)
    if configured:
        return configured
    factor = {
        ("°F", "°C"): (5 / 9, -160 / 9),
        ("km/h", "m/s"): (1 / 3.6, 0),
        ("mph", "m/s"): (0.44704, 0),
        ("kn", "m/s"): (1852 / 3600, 0),
        ("inch", "mm"): (25.4, 0),
    }.get((unit, canonical))
    if factor and rule.get("type", "number") == "number":
        return factor
    raise ValueError("Unknown or incompatible returned unit")


def value_at(value, rule, source_unit, convert, offset):
    typed = [None] * len(VALUE_TYPES)
    if value is None:
        return typed, "source_null"
    kind = rule.get("type", "number")
    if kind == "number" and finite(value):
        value = (
            (value - 32) * 5 / 9
            if (source_unit, rule["unit"]) == ("°F", "°C") and (source_unit not in rule.get("conversions", {}))
            else value * convert[0] + convert[1]
        )
        if not finite(value) or value < rule.get("min", -math.inf) or value > rule.get("max", math.inf):
            raise ArithmeticError("Value outside the allowed domain")
    elif kind == "timestamp" and isinstance(value, (int, str)) and type(value) is not bool:
        value = source_time(value, offset).astimezone(UTC)
    elif not (
        (kind == "code" and type(value) is int and -(2**31) <= value < 2**31)
        or (kind == "boolean" and type(value) is bool)
        or (kind == "text" and isinstance(value, str))
    ):
        raise ValueError("Unexpected value type")
    typed[VALUE_TYPES.index(kind)] = value
    quality = "valid"
    if kind == "number" and (
        value < rule.get("plausible_min", -math.inf) or value > rule.get("plausible_max", math.inf)
    ):
        quality = "flagged"
    return typed, quality


def member_field(field, rule, returned):
    if rule.get("member_kind") != "member":
        return [(field, rule.get("member_kind", "deterministic"), None)] if field in returned else []
    result = []
    match_member = re.compile(re.escape(field) + r"_member(\d+)$")
    for name in returned:
        match = match_member.fullmatch(name)
        if name == field and rule.get("control_member") is not None:
            number = rule["control_member"]
        elif match:
            number = int(match[1])
        else:
            continue
        result.append((name, "member", number))
    return result


class Load:
    def __init__(self, db, slice_, attempt, available, correction):
        self.db = db
        self.slice = slice_
        self.attempt = attempt
        self.fetch_id = attempt["fetch_id"]
        self.available = available
        self.correction = correction
        self.stamp = max(now(), attempt["fetched_at"])
        self.issues = []
        self.rows = []
        self.coverage = []
        self.points = []
        self.blocks = []
        self.accepted = []
        self.revisions = {}
        self.occurrences = {}

    def issue(self, code, site=None, field=None, valid=None, detail=None, block=None):
        self.issues.append(
            (
                self.fetch_id,
                site,
                block or self.slice["block"],
                field,
                valid or self.slice["start"],
                valid or self.slice["end"],
                code,
                detail,
            )
        )

    def catalog(self):
        product = self.slice["product"]
        self.product_id = put(
            self.db,
            "silver.product",
            "product_id",
            [key(["product", product["code"]]), self.slice["category"], product["code"], product["vintage"]],
        )
        self.profile_id = anchor_profile(self.db, self.slice["profile"])
        kind = product["vintage"]
        if kind == "exact_run":
            if not self.slice["run"] or self.slice["selector_kind"] != "model":
                raise ValueError("No verified run selector for exact run vintage")
            source_key = digest([self.slice["selector"], self.slice["model_id"], self.slice["run"]])
        elif kind == "archive":
            source_key = self.slice["selector"]
        elif kind == "capture":
            source_key = str(self.fetch_id)
        else:
            raise ValueError("Unknown vintage kind")
        self.vintage_id = put(
            self.db,
            "silver.vintage",
            "vintage_id",
            [
                key(["vintage", self.product_id, kind, source_key]),
                self.product_id,
                kind,
                source_key,
                self.slice["run"] if kind == "exact_run" else None,
                self.attempt["fetched_at"] if kind == "capture" else None,
            ],
        )

    def parse(self, payload):
        requested = self.slice["sites"]
        response = payload if isinstance(payload, list) else [payload]
        block = self.slice["block"]
        located = defaultdict(list)
        full = len(response) == len(requested)
        for index, point in enumerate(response):
            number = (
                point.get("location_id", index if full else None)
                if isinstance(point, dict)
                else index
                if full
                else None
            )
            if type(number) is int and 0 <= number < len(requested):
                located[number].append((index, point))
            else:
                self.issue("unmatched_result", detail={"result_index": index, "location_id": number})
        for request_index, site in enumerate(requested):
            matches = located[request_index]
            if len(matches) != 1:
                reason = "ambiguous_location" if matches or (len(response) and not full) else "missing_location"
                self.bad_point(site, reason)
                continue
            index, point = matches[0]
            path = f"$[{index}]" if isinstance(payload, list) else "$"
            if not isinstance(point, dict) or point.get("error"):
                self.bad_point(site, "invalid_location")
                continue
            try:
                lat = coordinate(point, "latitude", -90, 90)
                lon = coordinate(point, "longitude", -180, 180)
            except ValueError:
                self.bad_point(site, "invalid_location")
                continue
            elevation = point.get("elevation") if finite(point.get("elevation")) else None
            name = point.get("timezone", self.slice["timezone"])
            try:
                zone = ZoneInfo(name) if name not in {"GMT", "GMT+0"} else ZoneInfo("UTC")
                offset = point.get("utc_offset_seconds", 0 if zone.key == "UTC" else None)
                if type(offset) is not int or abs(offset) >= 86400:
                    raise ValueError("Invalid offset")
            except (ZoneInfoNotFoundError, ValueError, TypeError):
                self.bad_point(site, "invalid_timezone")
                continue
            generation = point.get("generationtime_ms")
            self.points.append(
                [
                    self.fetch_id,
                    index,
                    request_index,
                    site["location_key"],
                    site["display_name"],
                    self.slice["timezone"],
                    name,
                    point.get("timezone_abbreviation"),
                    path,
                    offset,
                    site["latitude"],
                    site["longitude"],
                    lat,
                    lon,
                    elevation,
                    generation if finite(generation) else None,
                ]
            )
            for code, data in point.items():
                if not isinstance(data, dict) or "time" not in data:
                    continue
                raw = data["time"] if isinstance(data["time"], list) else [data["time"]]
                interval_s = data.get("interval")
                self.blocks.append(
                    [
                        self.fetch_id,
                        index,
                        code,
                        len(raw),
                        interval_s if type(interval_s) is int else None,
                        f"{path}.{code}" if code.isidentifier() else f"{path}.{json_text(code)}",
                        f"{path}.{code}_units"
                        if isinstance(point.get(code + "_units"), dict) and code.isidentifier()
                        else None,
                        json_text(raw[0]) if raw else None,
                        json_text(raw[-1]) if raw else None,
                        sorted(set(data) - {"time", "interval"}),
                    ]
                )
            if not isinstance(point.get(block), dict):
                self.bad_point(site, "missing_block")
                continue
            point_id = put(
                self.db,
                "silver.source_point",
                "source_point_id",
                [
                    key(["point", lat, lon, elevation, self.slice["model_id"]]),
                    lat,
                    lon,
                    elevation,
                    self.db.execute(
                        "SELECT grid_version FROM ops.forecast_model WHERE model_version_id = ?",
                        [self.slice["model_id"]],
                    ).fetchone()[0]
                    if self.slice["model_id"]
                    else None,
                ],
            )
            self.parse_block(site, point_id, index, point[block], point.get(block + "_units"), zone, name, offset)

    def bad_point(self, site, reason, detail=None):
        self.issue(reason, site["location_key"], detail=detail)
        self.coverage.extend(
            row
            for field, rule in self.slice["fields"].items()
            for row in empty_coverage(self.slice, site["location_key"], field, rule, reason, self.fetch_id)
        )

    def parse_block(self, site, point_id, index, data, units, zone, timezone_name, offset):
        block = self.slice["block"]
        current = block == "current"
        step = data.get("interval") if current else self.slice["step"]
        try:
            times = [data["time"]] if current else data["time"]
            if not isinstance(times, list) or (current and (type(step) is not int or step <= 0)):
                raise ValueError("Invalid time axis")
            axis = [normalized_axis(block, raw, offset) for raw in times]
            if any(a[0] >= b[0] for a, b in pairwise(axis)):
                raise ValueError("Duplicate or unordered timestamps")
        except (KeyError, ValueError, OverflowError, OSError) as exc:
            self.bad_point(site, "invalid_time_axis", detail=str(exc))
            return
        if not isinstance(units, dict):
            units = {}
        base = {
            "site": site["location_key"],
            "point": point_id,
            "index": index,
            "block": block,
            "zone": zone,
            "timezone_name": timezone_name,
            "axis": axis,
            "step": step,
            "current": current,
        }
        known = {"time", "interval"}
        for field, rule in self.slice["fields"].items():
            entries = member_field(field, rule, data)
            known.update(name for name, _, _ in entries)
            self.parse_field(base, field, rule, entries, data, units)
        for name in sorted(data.keys() - known):
            self.issue("unmapped_field", site["location_key"], name)

    def parse_field(self, base, field, rule, entries, data, units):
        if rule.get("support", "supported") != "supported":
            self.coverage.append(empty_row(self.slice, base["site"], field, rule, "unsupported", self.fetch_id))
            return
        upper = horizon_end(self.slice, rule)
        expected = list(
            slots(self.slice["start"], upper, ZoneInfo(self.slice["timezone"]), self.slice["block"], self.slice["step"])
        )
        if upper < self.slice["end"]:
            self.coverage.append(
                [
                    self.slice["id"],
                    base["site"],
                    base["block"],
                    field,
                    upper,
                    self.slice["end"],
                    self.fetch_id,
                    "outside_horizon",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    len(rule["members"]) if "members" in rule else None,
                    None,
                ]
            )
        if upper == self.slice["start"]:
            return
        if base["current"]:
            plan_times = {time[0] for time in base["axis"] if self.slice["start"] <= time[0] < upper}
            expected = list(plan_times)
        elif base["block"] in PERIODS:
            requested_zone = ZoneInfo(self.slice["timezone"])
            by_day = {day: when for when, day, _ in base["axis"]}
            expected = [by_day.get(when.astimezone(requested_zone).date(), when) for when in expected]
            plan_times = set(expected)
        else:
            plan_times = set(expected)
        member_count = len(rule["members"]) if "members" in rule else None
        observation = []
        shape = []
        if not entries:
            self.issue("missing_field", base["site"], field)
        else:
            for api_field, _, _ in entries:
                raw = data[api_field]
                values = [raw] if base["current"] and not isinstance(raw, (list, dict)) else raw
                shape.append(
                    (
                        api_field,
                        len(base["axis"]),
                        len(values)
                        if isinstance(values, list) and not (base["current"] and isinstance(raw, list))
                        else -1,
                    )
                )
        self.db.execute(
            "CREATE OR REPLACE TEMP TABLE field_shape (api_field VARCHAR, time_count INTEGER, value_count INTEGER)"
        )
        insert_many(self.db, "field_shape", shape)
        mismatch = dict(
            (name, count)
            for name, count in self.db.execute(
                "SELECT api_field, value_count FROM field_shape WHERE time_count <> value_count"
            ).fetchall()
        )
        numbers = [number for _, kind, number in entries if kind == "member"]
        for api_field, kind, member in entries:
            source_unit = units.get(api_field, units.get(rule.get("units_key", field)))
            valid_shape = api_field not in mismatch
            if not valid_shape:
                self.issue(
                    "array_length_mismatch",
                    base["site"],
                    api_field,
                    detail={"expected": len(base["axis"]), "returned": mismatch[api_field]},
                )
            allowed_member = kind != "member" or (
                numbers.count(member) == 1 and ("members" not in rule or member in rule["members"])
            )
            if not allowed_member:
                self.issue("invalid_member", base["site"], api_field)
                continue
            try:
                if not isinstance(source_unit, str):
                    raise ValueError("Missing returned unit")
                transform = conversion(source_unit, rule)
            except ValueError:
                transform = None
                self.issue("invalid_unit", base["site"], api_field, detail=source_unit)
            if valid_shape and transform is not None and plan_times:
                mapping_id, measure_id, mapping_version = field_mapping(
                    self.db, self.product_id, self.slice["selector"], base["block"], api_field, rule
                )
                identity = [
                    self.product_id,
                    key(["site", base["site"]]),
                    measure_id,
                    self.profile_id,
                    self.slice["selector"],
                    base["block"],
                    base["step"],
                    kind,
                    member,
                    rule.get("ensemble_statistic"),
                    rule.get("ensemble_percentile"),
                    rule.get("scenario_code"),
                    rule.get("fixed_lead_hours"),
                ]
                series_id = put(
                    self.db, "silver.series", "series_id", [key(["series", identity]), *identity, json_text(identity)]
                )
                self.load_revisions(series_id, min(plan_times), max(plan_times))
            for i, (at, day, offset) in enumerate(base["axis"]):
                if at not in plan_times:
                    if not self.slice.get("full_response"):
                        self.issue("unexpected_slot", base["site"], api_field, at)
                    continue
                if not valid_shape or transform is None:
                    observation.append((at, member, "invalid", False))
                    continue
                value = data[api_field] if base["current"] else data[api_field][i]
                try:
                    start, end = interval(rule, at, day, offset, base["block"], base["zone"], base["step"])
                    typed, quality = value_at(value, rule, source_unit, transform, offset)
                except (ValueError, TypeError, ArithmeticError, OSError) as exc:
                    code = "invalid_value" if not isinstance(exc, ArithmeticError) else "out_of_domain"
                    self.issue(code, base["site"], api_field, at, detail=str(exc))
                    observation.append((at, member, "invalid", True))
                    continue
                if quality == "flagged":
                    self.issue("unusual_value", base["site"], api_field, at)
                revision = self.accept_revision(
                    series_id,
                    at,
                    [start, end, *typed, quality, base["point"]],
                    [self.fetch_id, base["index"], base["block"], api_field, i, mapping_id],
                    mapping_version,
                    base["site"],
                )
                if revision:
                    if revision > 0:
                        self.rows.append(
                            [
                                series_id,
                                self.vintage_id,
                                at,
                                revision,
                                start,
                                end,
                                self.available,
                                self.stamp,
                                day,
                                base["timezone_name"],
                                offset,
                                *typed,
                                source_unit,
                                quality,
                                rule.get("sampling_origin", "unknown"),
                                base["point"],
                                self.slice["model_id"] if self.slice["selector_kind"] == "model" else None,
                                mapping_id,
                                self.fetch_id,
                                base["index"],
                                base["block"],
                                api_field,
                                i,
                            ]
                        )
                    self.accepted.append(
                        [
                            self.fetch_id,
                            base["index"],
                            base["block"],
                            api_field,
                            i,
                            mapping_id,
                            series_id,
                            self.vintage_id,
                            at,
                            abs(revision),
                        ]
                    )
                    observation.append((at, member, quality, True))
                else:
                    observation.append((at, member, "invalid", True))
        self.db.execute("""
            CREATE OR REPLACE TEMP TABLE expected_slot (valid_at TIMESTAMPTZ PRIMARY KEY);
            CREATE OR REPLACE TEMP TABLE observed_slot (
                valid_at TIMESTAMPTZ, member INTEGER, quality VARCHAR, has_value BOOLEAN)
        """)
        insert_many(self.db, "expected_slot", [(value,) for value in expected])
        insert_many(self.db, "observed_slot", observation)
        count, returned, usable, nulls, invalid, members = self.db.execute(
            """
            WITH matched AS (
                SELECT e.valid_at, count(*) FILTER (WHERE o.has_value) AS n_returned,
                       count(*) FILTER (WHERE o.quality IN ('valid', 'flagged')) AS n_usable,
                       bool_or(o.quality = 'source_null') AS has_null, bool_or(o.quality = 'invalid') AS invalid
                FROM expected_slot e LEFT JOIN observed_slot o USING (valid_at)
                GROUP BY e.valid_at
            )
            SELECT count(*), count(*) FILTER (WHERE n_returned > 0),
                   count(*) FILTER (WHERE n_usable >= ?),
                   count(*) FILTER (WHERE has_null), count(*) FILTER (WHERE invalid),
                   (SELECT count(DISTINCT member) FROM observed_slot WHERE quality IN ('valid', 'flagged'))
            FROM matched
        """,
            [member_count or 1],
        ).fetchone()
        status = (
            "missing_field" if not entries else "complete" if count and usable == count and not invalid else "partial"
        )
        if status == "partial" and usable < count:
            self.issue("incomplete_slots", base["site"], field, detail={"expected": count, "usable": usable})
        self.coverage.append(
            [
                self.slice["id"],
                base["site"],
                base["block"],
                field,
                self.slice["start"],
                upper,
                self.fetch_id,
                status,
                count or (1 if base["current"] else 0),
                returned,
                usable,
                nulls,
                invalid,
                (count or (1 if base["current"] else 0)) - returned,
                member_count,
                members if rule.get("member_kind") == "member" else None,
            ]
        )

    def load_revisions(self, series_id, start, end):
        if series_id in self.revisions:
            return
        attempts = f"bronze.{self.slice['category']}"
        records = self.db.execute(
            f"""
            SELECT f.valid_at, f.revision_no, f.window_start, f.window_end, f.value_number, f.value_code,
                   f.value_timestamp, f.value_boolean, f.value_text, f.quality_status, f.source_point_id,
                   f.source_available_at, a.fetched_at, m.mapping_version
            FROM silver.series_value f JOIN silver.field_mapping m USING (mapping_id)
            JOIN {attempts} a USING (fetch_id)
            WHERE f.series_id = ? AND f.vintage_id = ? AND f.valid_at >= ? AND f.valid_at <= ?
            ORDER BY f.revision_no DESC
        """,
            [series_id, self.vintage_id, start, end],
        ).fetchall()
        self.revisions[series_id] = defaultdict(list)
        for at, *revision in records:
            self.revisions[series_id][at].append(revision)
        for *source, revision in self.db.execute(
            """
            SELECT fetch_id, result_index, block_code, api_field, sample_index, mapping_id, revision_no
            FROM silver.series_value_source WHERE fetch_id = ? AND series_id = ?
        """,
            [self.fetch_id, series_id],
        ).fetchall():
            self.occurrences[tuple(source)] = revision

    def accept_revision(self, series_id, at, comparable, occurrence, mapping_version, site):
        source = tuple(occurrence)
        if source in self.occurrences:
            return -self.occurrences[source]
        records = self.revisions[series_id][at]
        if records:
            latest = records[0]
            previous_time = latest[10] or latest[11]
            is_correction = self.correction and self.available and self.available > previous_time
            for revision in records:
                if revision[1:10] == comparable and (revision == latest or not is_correction):
                    return -revision[0]
            if mapping_version <= latest[12] and not is_correction:
                self.issue("unordered_conflict", site, occurrence[3], at, {"previous_revision": latest[0]})
                return None
            number = latest[0] + 1
        else:
            number = 1
        records.insert(0, [number, *comparable, self.available, self.attempt["fetched_at"], mapping_version])
        self.occurrences[source] = number
        return number

    def publish(self):
        source = f"bronze.{self.slice['category']}_block"
        self.db.execute("""
            CREATE OR REPLACE TEMP TABLE fact_stage AS SELECT * FROM silver.series_value WHERE false;
            CREATE OR REPLACE TEMP TABLE source_stage AS SELECT * FROM silver.series_value_source WHERE false;
        """)
        insert_many(self.db, "fact_stage", self.rows)
        insert_many(self.db, "source_stage", self.accepted)
        invalid = self.db.execute(f"""
            SELECT count(*)
            FROM fact_stage f
            JOIN silver.series s USING (series_id)
            JOIN silver.measure m USING (measure_id)
            JOIN silver.vintage v USING (vintage_id)
            JOIN silver.field_mapping fm USING (mapping_id)
            LEFT JOIN {source} b ON (f.fetch_id, f.result_index, f.block_code) = (b.fetch_id, b.result_index, b.block_code)
            LEFT JOIN ops.forecast_model model ON f.source_model_version_id = model.model_version_id
            WHERE s.product_id <> v.product_id OR fm.product_id <> s.product_id OR fm.measure_id <> m.measure_id
               OR b.fetch_id IS NULL OR NOT list_contains(b.returned_fields, f.api_field)
               OR f.sample_index >= b.sample_count OR f.sample_index < 0
               OR (f.source_model_version_id IS NOT NULL AND model.model_version_id IS NULL)
               OR (f.quality_status <> 'source_null' AND NOT CASE m.value_type
                    WHEN 'number' THEN f.value_number IS NOT NULL AND isfinite(f.value_number)
                    WHEN 'code' THEN f.value_code IS NOT NULL
                    WHEN 'timestamp' THEN f.value_timestamp IS NOT NULL
                    WHEN 'boolean' THEN f.value_boolean IS NOT NULL
                    WHEN 'text' THEN f.value_text IS NOT NULL ELSE false END)
        """).fetchone()[0]
        if invalid:
            raise ValueError(f"Silver stage has {invalid} invalid fact rows")
        published_at = max(now(), self.stamp)
        self.db.execute(
            """
            INSERT INTO silver.series_value
            SELECT * REPLACE (? AS warehouse_available_at) FROM fact_stage
        """,
            [published_at],
        )
        self.db.execute("""
            INSERT INTO silver.series_value_source SELECT * FROM source_stage
            ON CONFLICT (fetch_id, result_index, block_code, api_field, sample_index, mapping_id) DO NOTHING
        """)
        return self.update_coverage()

    def update_coverage(self):
        statuses = {row[7] for row in self.coverage}
        informative = statuses - {"unsupported", "outside_horizon", "unverified"}
        if informative in ({"complete"}, {"empty_result"}):
            status = "complete"
        elif statuses and not informative:
            status = "outside_horizon" if "outside_horizon" in statuses else "unsupported"
        else:
            status = "partial"
        table = f"bronze.{self.slice['category']}"
        latest = self.db.execute(
            f"""
            SELECT fetch_id FROM {table} WHERE slice_id = ? ORDER BY attempt_no DESC LIMIT 1
        """,
            [self.slice["id"]],
        ).fetchone()
        if latest and latest[0] == self.fetch_id:
            append_coverage(self.db, self.slice, self.coverage, status)
            self.db.execute(
                """
                INSERT INTO ops.capability_evidence
                SELECT c.slice_id, s.capability_id, c.fetch_id, c.location_key, ?, c.usable_slots, c.usable_members
                FROM silver.coverage c JOIN ops.slice_capability s USING (slice_id)
                JOIN ops.api_capability p USING (capability_id, block_code, api_field)
                WHERE c.slice_id = ? AND c.coverage_status = 'complete' AND c.usable_slots > 0
                ON CONFLICT DO NOTHING
            """,
                [self.attempt["fetched_at"], self.slice["id"]],
            )
        return status

    def reference(self, payload):
        category = self.slice["category"]
        if category == "geocoding":
            self.geocode(payload)
        else:
            self.elevations(payload)
        return self.update_coverage()

    def geocode(self, payload):
        params = self.slice["params"]
        lookup = "id" in params
        results = [payload] if lookup and "id" in payload else payload.get("results", []) if not lookup else None
        if (
            not isinstance(results, list)
            or len(results) > (1 if lookup else int(params.get("count", 10)))
            or (not results and not lookup and "results" not in payload and not set(payload) <= {"generationtime_ms"})
        ):
            self.issue("invalid_geocoding_response")
            self.coverage.append(empty_row(self.slice, "__query__", "geocoding", {}, "invalid_response", self.fetch_id))
            return
        usable = 0
        seen = set()
        language = str(params.get("language", "en"))
        for index, place in enumerate(results):
            try:
                place_id = place.get("id")
                name = place.get("name")
                country = place.get("country_code")
                zone = place.get("timezone")
                if (
                    type(place_id) is not int
                    or not 0 < place_id < 2**63
                    or place_id in seen
                    or not isinstance(name, str)
                    or not name
                    or (lookup and place_id != int(params["id"]))
                    or (country is not None and not isinstance(country, str))
                    or (zone is not None and not isinstance(zone, str))
                ):
                    raise ValueError("Invalid place")
                lat = coordinate(place, "latitude", -90, 90)
                lon = coordinate(place, "longitude", -180, 180)
            except (AttributeError, ValueError, TypeError):
                self.issue("invalid_geocode_place", field=str(index))
                continue
            path = "$" if lookup else f"$.results[{index}]"
            put(
                self.db,
                "bronze.geocoding_result",
                "fetch_id, result_index",
                [self.fetch_id, index, place_id, lat, lon, name, language, country, zone, path],
            )
            put(self.db, "silver.place", "place_id", [place_id, lat, lon, country, zone])
            put(self.db, "silver.place_name", "place_id, language, place_name", [place_id, language, name])
            put(self.db, "silver.geocode_match", "fetch_id, result_index", [self.fetch_id, index, place_id])
            usable += 1
            seen.add(place_id)
        status = "empty_result" if not results else "complete" if usable == len(results) else "partial"
        self.coverage.append(
            [
                self.slice["id"],
                "__query__",
                "result",
                "geocoding",
                self.slice["start"],
                self.slice["end"],
                self.fetch_id,
                status,
                1 if lookup else None,
                len(results),
                usable,
                0,
                len(results) - usable,
                0 if lookup else None,
                None,
                None,
            ]
        )

    def elevations(self, payload):
        values = payload.get("elevation")
        sites = self.slice["sites"]
        if not isinstance(values, list) or len(values) != len(sites):
            reason = (
                "missing_elevation"
                if values == []
                else "ambiguous_elevation"
                if isinstance(values, list)
                else "invalid_elevation_response"
            )
            self.issue(
                reason, detail={"expected": len(sites), "returned": len(values) if isinstance(values, list) else None}
            )
            self.coverage.extend(
                empty_row(self.slice, site["location_key"], "elevation", {}, reason, self.fetch_id) for site in sites
            )
            return
        for index, (site, value) in enumerate(zip(sites, values, strict=True)):
            valid = finite(value)
            location = site["location_key"]
            if valid:
                put(
                    self.db,
                    "bronze.elevation_result",
                    "fetch_id, request_index",
                    [self.fetch_id, index, f"$.elevation[{index}]", site["latitude"], site["longitude"], value],
                )
                put(
                    self.db,
                    "silver.elevation",
                    "site_id, fetch_id, request_index",
                    [key(["site", location]), self.fetch_id, index, value, self.stamp],
                )
            else:
                self.issue("missing_elevation" if value is None else "invalid_elevation", location)
            self.coverage.append(
                [
                    self.slice["id"],
                    location,
                    "result",
                    "elevation",
                    self.slice["start"],
                    self.slice["end"],
                    self.fetch_id,
                    "complete" if valid else "missing_elevation" if value is None else "partial",
                    1,
                    1,
                    int(valid),
                    int(value is None),
                    int(value is not None and not valid),
                    0,
                    None,
                    None,
                ]
            )


def normalize(db, slice_, fetch_id, *, available=None, correction=False):
    attempt = read_attempt(db, slice_, fetch_id)
    if not checksum_matches(attempt):
        return unavailable(db, slice_, "checksum_mismatch", fetch_id)
    if attempt["fetch_error"] or not attempt["http_status"] or not 200 <= attempt["http_status"] < 300:
        return unavailable(
            db,
            slice_,
            "fetch_failed",
            fetch_id,
            {"http_status": attempt["http_status"], "error": attempt["fetch_error"]},
        )
    reference = slice_["category"] in {"elevation", "geocoding"}
    try:
        payload = strict_json(attempt["response_body"])
        if not isinstance(payload, dict if reference else (list, dict)):
            raise ValueError("Invalid response structure")
        if isinstance(payload, dict) and payload.get("error"):
            raise ValueError("API reports an error")
    except (ValueError, TypeError, RecursionError) as exc:
        return unavailable(db, slice_, "invalid_response", fetch_id, str(exc))
    evidence = db.execute(
        """
        SELECT source_available_at, ordered_correction FROM ops.availability_evidence WHERE fetch_id = ?
    """,
        [fetch_id],
    ).fetchone()
    if evidence:
        available, correction = evidence
    available = instant(available) if available else None
    if correction and not available:
        raise ValueError("An ordered source correction requires evidence of source availability")
    if available and (available > attempt["fetched_at"] or (slice_["run"] and available < slice_["run"])):
        raise ValueError("Source availability falls outside the verified time bounds")
    batch = Load(db, slice_, attempt, available, correction)
    try:
        with transaction(db):
            if reference:
                status = batch.reference(payload)
            else:
                batch.catalog()
                batch.parse(payload)
                parent = f"bronze.{slice_['category']}"
                db.execute(f"CREATE OR REPLACE TEMP TABLE point_stage AS SELECT * FROM {parent}_point WHERE false")
                db.execute(f"CREATE OR REPLACE TEMP TABLE block_stage AS SELECT * FROM {parent}_block WHERE false")
                insert_many(db, "point_stage", batch.points)
                insert_many(db, "block_stage", batch.blocks)
                db.execute(
                    f"INSERT INTO {parent}_point SELECT * FROM point_stage "
                    "ON CONFLICT (fetch_id, result_index) DO NOTHING"
                )
                db.execute(
                    f"INSERT INTO {parent}_block SELECT * FROM block_stage "
                    "ON CONFLICT (fetch_id, result_index, block_code) DO NOTHING"
                )
                status = batch.publish()
            record_issues(db, slice_, batch.issues)
    except Exception as exc:
        batch.issue("load_invariant", detail=str(exc))
        record_issues(db, slice_, batch.issues)
        raise
    return status
