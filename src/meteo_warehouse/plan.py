import json
import math
import tomllib
from datetime import UTC, date, datetime, time, timedelta
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from .db import CATEGORIES, PACKAGE, digest, instant, insert, json_text, key, now, transaction, uuid_key


CADENCES = {"hourly": 3600, "minutely_15": 900, "six_hourly": 21600, "daily": 86400,
            "weekly": 604800, "monthly": None, "current": None}
PROFILE_OPTIONS = {"cell_selection", "elevation", "tilt", "azimuth", "disable_bias_correction",
                   "temporal_resolution", "domains"}
CONTROL_PARAMS = {"temperature_unit", "wind_speed_unit", "precipitation_unit", "timeformat",
                  "forecast_days", "past_days", "forecast_hours", "past_hours", "forecast_minutely_15",
                  "past_minutely_15", "name", "id", "count", "language", "countryCode",
                  "ensemble", "ensemble_perturbations", "temporal_resolution", "domains"}
WINDOW_PARAMS = {"start_hour", "end_hour", "start_date", "end_date", "start_minutely_15", "end_minutely_15"}
NO_SELECTOR = "__default__"
LIMIT_KEYS = ("max_locations", "max_fields", "max_days")
CALENDAR_BLOCKS = {"daily", "weekly", "monthly"}


def load_config(path=None):
    with (open(path, "rb") if path else PACKAGE.joinpath("starter.toml").open("rb")) as stream:
        return tomllib.load(stream)


def resolve_job(config, name):
    job = config["jobs"][name]
    profile = config.get("profiles", {})[job["request_profile"]] if "request_profile" in job else {}
    return {**profile, **job, "params": {**profile.get("params", {}), **job.get("params", {})},
            "profile": {**profile.get("profile", {}), **job.get("profile", {})}}


def local_midnight(day, zone):
    return datetime.combine(day, time(), zone).astimezone(UTC)


def datetime_at(value, zone):
    if isinstance(value, date) and not isinstance(value, datetime):
        return local_midnight(value, zone)
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    return (result if result.tzinfo else result.replace(tzinfo=zone)).astimezone(UTC)


def period_end(day, cadence):
    if cadence == "monthly":
        return (day.replace(day=28) + timedelta(days=4)).replace(day=1)
    return day + timedelta(days=7 if cadence == "weekly" else 1)


def slots(start, end, zone, block, step):
    if block in {"daily", "weekly", "monthly"}:
        day = start.astimezone(zone).date()
        while (slot := local_midnight(day, zone)) < end:
            if slot >= start:
                yield slot
            day = period_end(day, block)
    elif step:
        while start < end:
            yield start
            start += timedelta(seconds=step)


def public_url(url, *, metadata=False):
    value = urlsplit(url)
    if (value.scheme != "https" or value.username or value.password or value.port or value.query
            or value.fragment or not value.hostname or not value.hostname.endswith(".open-meteo.com")
            or value.hostname.startswith("customer-") or not value.path.startswith("/")):
        raise ValueError(f"Expected a public Open-Meteo {'metadata' if metadata else 'API'} URL")
    return value


def site_data(name, entry):
    lat, lon = float(entry["latitude"]), float(entry["longitude"])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError(f"Invalid coordinates for {name}")
    return {"location_key": name, "latitude": lat, "longitude": lon,
            "display_name": entry.get("display_name", name), "place_id": entry.get("place_id")}


def put_site(db, site):
    name = site["location_key"]
    identity = key(["site", name])
    values = [identity, name, site["latitude"], site["longitude"], site["display_name"], site["place_id"]]
    old = db.execute("SELECT latitude, longitude FROM silver.site WHERE site_id = ?", [identity]).fetchone()
    if old and old != tuple(values[2:4]):
        raise ValueError(f"Location {name} already identifies different coordinates")
    if not old:
        if site["place_id"] is not None and not db.execute(
                "SELECT 1 FROM silver.place WHERE place_id = ?", [site["place_id"]]).fetchone():
            raise ValueError(f"Place {site['place_id']} must be ingested before it is assigned to {name}")
        insert(db, "silver.site", values)
    return identity


def profile_data(params, overrides=None, *, calendar=True):
    # Output units and encodings are normalized separately from value-affecting parameters.
    options = {name: value for name, value in params.items() if name in PROFILE_OPTIONS}
    options.update(overrides or {})
    if str(options.get("elevation", "")).lower() == "nan":
        options["elevation"] = "nan"
    return {"timezone": params.get("timezone", "UTC") if calendar else "UTC", "options": options}


def model_metadata(db, model):
    if not model:
        return None
    data = dict(model)
    ident = key(["model", data])
    if data.get("metadata_url"):
        public_url(data["metadata_url"], metadata=True)
    if not db.execute("SELECT 1 FROM ops.forecast_model WHERE model_version_id = ?", [ident]).fetchone():
        insert(db, "ops.forecast_model", [ident, data.get("provider"), data["code"], data.get("domain", "global"),
               data.get("grid_version"), data.get("spatial_resolution"), data.get("resolution_unit"),
               data.get("horizon_hours"), data.get("update_interval_seconds"), data.get("metadata_url"),
               datetime_at(data["effective_from"], UTC) if "effective_from" in data else None,
               datetime_at(data["effective_to"], UTC) if "effective_to" in data else None])
    return ident


def active_period(rule, at):
    start = datetime_at(rule["effective_from"], UTC) if "effective_from" in rule else None
    end = datetime_at(rule["effective_to"], UTC) if "effective_to" in rule else None
    if start is not None and end is not None and start >= end:
        raise ValueError("Capability effective dates must form a nonempty interval")
    return (start is None or start <= at) and (end is None or at < end)


def choose_fields(config, job, at):
    fields = {}
    for name in job.get("fields", []):
        spec = dict(config["fields"][name])
        api_field = spec.pop("api_field", name)
        spec.setdefault("measure_code", name)
        spec.update(job.get("capability_defaults", {}))
        spec.update(job.get("field_overrides", {}).get(name, {}))
        versions = spec.pop("versions", None)
        if versions is not None:
            applicable = [spec | version for version in versions if active_period(spec | version, at)]
            if len(applicable) > 1:
                raise ValueError(f"Overlapping capability versions for {api_field}")
            spec = applicable[0] if applicable else spec | {"support": "unverified"}
        elif not active_period(spec, at):
            spec["support"] = "unverified"
        if spec.get("support", "supported") not in {"supported", "unsupported", "unverified"}:
            raise ValueError(f"Invalid support status for {api_field}")
        if spec.get("horizon_reference", "run") not in {"run", "requested_window", "trigger_run"}:
            raise ValueError(f"Invalid horizon reference for {api_field}")
        horizon = spec.get("horizon_hours")
        if horizon is not None and (type(horizon) not in {int, float} or not math.isfinite(horizon) or horizon <= 0):
            raise ValueError(f"Invalid horizon for {api_field}")
        if api_field in fields:
            raise ValueError(f"Duplicate field {api_field}")
        fields[api_field] = spec
    return fields


def request_limits(product, fields):
    rules = [product.get("request_limits", {})]
    rules.extend(field.get("request_limits", {}) for field in fields.values()
                 if field.get("support", "supported") == "supported")
    result = {}
    for rule in rules:
        for name, value in rule.items():
            if name not in LIMIT_KEYS or type(value) is not int or value <= 0:
                raise ValueError(f"Invalid API request limit {name}={value}")
            result[name] = min(result.get(name, value), value)
    return result


def plan_job(db, config, name, *, anchor=None, start=None, end=None, run=None, trigger=None):
    job = resolve_job(config, name)
    product = dict(config["products"][job["product"]])
    product["code"] = job["product"]
    category = product["category"]
    if category not in CATEGORIES:
        raise ValueError(f"Unknown Bronze category {category}")
    public_url(product["url"])
    if product.get("selector_param", "models") not in {"models", "domains", None, False}:
        raise ValueError("Expected a public model or domain selector parameter")
    mode = job["mode"]
    if mode not in {"live", "archive", "exact"}:
        raise ValueError(f"Invalid planning mode for {name}")
    vintage = product["vintage"]
    if (mode == "exact") != (vintage == "exact_run"):
        raise ValueError("Exact run jobs require an exact-run product")
    block = job.get("block", "result")
    if block not in CADENCES and not (block == "result" and category in {"geocoding", "elevation"}):
        raise ValueError(f"Unsupported block {block}")
    anchor = instant(anchor) if anchor else now()
    timezone = job.get("timezone", "UTC")
    zone = ZoneInfo(timezone)
    if category not in {"elevation", "geocoding"} and block not in {"current", "daily", "weekly", "monthly"} and timezone != "UTC":
        raise ValueError("Subdaily requests must use UTC to preserve unambiguous slot keys")
    if block == "current":
        step = job.get("step_seconds")
    else:
        step = job.get("step_seconds", CADENCES.get(block))
    if step is not None and step < 1:
        raise ValueError("step_seconds must be positive")
    run = instant(run) if run else None
    if (mode == "exact") != (run is not None) or (run and (run.second or run.microsecond)):
        raise ValueError("Exact run jobs require a minute-aligned --run timestamp")
    selector = job.get("selector", NO_SELECTOR)
    if mode == "exact" and (selector == NO_SELECTOR or job.get("selector_kind") != "model"):
        raise ValueError("Exact run jobs require a single explicitly selected model")
    if "," in selector:
        raise ValueError("Each slice can request one selector")
    if (start is None) != (end is None):
        raise ValueError("Specify both --start and --end for an explicit window")
    if start is not None:
        start, end = datetime_at(start, zone), datetime_at(end, zone)
    elif mode == "archive":
        start, end = datetime_at(job["start"], zone), datetime_at(job["end"], zone)
    elif category in {"elevation", "geocoding"} or block == "current":
        window_seconds = job.get("window_seconds", 900)
        if window_seconds < 1:
            raise ValueError("Window size must be positive")
        start = datetime.fromtimestamp(math.floor(anchor.timestamp() / window_seconds) * window_seconds, UTC)
        end = start + timedelta(seconds=window_seconds)
    elif block == "daily":
        day = anchor.astimezone(zone).date() + timedelta(days=job.get("start_day_offset", 1))
        start = local_midnight(day, zone)
        end = local_midnight(day + timedelta(days=job.get("days", 1)), zone)
    else:
        if not step:
            raise ValueError("A rolling window requires a fixed subdaily cadence")
        start = (run + timedelta(hours=job.get("first_lead_hours", 0)) if run else
                 datetime.fromtimestamp(math.ceil(anchor.timestamp() / step) * step, UTC))
        end = start + timedelta(hours=job.get("hours", 24))
    if end <= start:
        raise ValueError("The window must have an exclusive end after its start")
    if block in {"daily", "weekly", "monthly"}:
        if any(when.astimezone(zone).time() != time() for when in (start, end)):
            raise ValueError("Calendar request windows must start and end at local midnight")
        if block == "monthly" and any(when.astimezone(zone).day != 1 for when in (start, end)):
            raise ValueError("Monthly windows must use the first day of the month")
        if block == "weekly" and (end.astimezone(zone).date() - start.astimezone(zone).date()).days % 7:
            raise ValueError("Weekly windows must span a whole number of weeks")
    elif block != "current" and step and ((end - start).total_seconds() % step or start.microsecond):
        raise ValueError("The window must contain a whole number of slots")
    all_fields = choose_fields(config, job, anchor)
    if not all_fields and block != "result":
        raise ValueError(f"No field mappings selected for {name}")
    raw_params = dict(job.get("params", {}))
    bad = set(raw_params) - CONTROL_PARAMS - PROFILE_OPTIONS
    if bad:
        raise ValueError(f"Unknown API parameters: {sorted(bad)}")
    if raw_params.get("timeformat", "iso8601") not in {"iso8601", "unixtime"}:
        raise ValueError("Only JSON responses with ISO or Unix timestamps are supported")
    sites = [site_data(site, config["sites"][site]) for site in job.get("sites", [])]
    if category != "geocoding" and (not sites or len({s["location_key"] for s in sites}) != len(sites)):
        raise ValueError("Each coordinate job requires distinct configured sites")
    if category == "geocoding" and (("name" in raw_params) == ("id" in raw_params)):
        raise ValueError("Geocoding requires either a place name or an ID")
    if category == "geocoding" and "id" in raw_params:
        endpoint = urlsplit(product["url"])
        product["url"] = endpoint._replace(path="/v1/get").geturl()
    limits = request_limits(product, all_fields)
    for field, rule in all_fields.items():
        rule["request_limits"] = request_limits(product, {field: rule})
    sizes = (job.get("batch_sites", len(sites) or 1), job.get("batch_fields", len(all_fields) or 1),
             job.get("batch_days", 14 if mode == "archive" else 366))
    if any(type(size) is not int or size < 1 for size in sizes):
        raise ValueError("Batch sizes must be positive")
    site_size, field_size, days = (min(size, limits.get(limit, size)) for size, limit in zip(sizes, LIMIT_KEYS))
    if mode == "live" and len(all_fields) > field_size and job.get("coherent", False):
        raise ValueError("A coherent live job needs all shared fields in one bundle")
    full_response = bool(run or job.get("window_params") == "relative")
    forward_days = raw_params.get("forecast_days", product.get("default_response_days", 0))
    past_days = raw_params.get("past_days", 0)
    if any(type(value) is not int or value < 0 for value in (forward_days, past_days)):
        raise ValueError("Response days must be nonnegative integers")
    request_days = forward_days + past_days
    if full_response and "max_days" in limits and (request_days < 1 or request_days > limits["max_days"]):
        raise ValueError("A full or relative response requires an API duration within the request limit")
    if block in {"weekly", "monthly"} and (end - start) > timedelta(days=days):
        raise ValueError("Use an aligned explicit window and batch_days covering it for weekly/monthly data")
    from .silver import empty_coverage

    ids = []
    with transaction(db):
        model = config.get("models", {}).get(job.get("model"))
        if model and not active_period(model, run or anchor):
            raise ValueError(f"Model version for {name} is outside its effective dates")
        model_id = model_metadata(db, model)
        if trigger and (not model_id or not db.execute("""
                SELECT 1 FROM bronze.model_update WHERE poll_id = ? AND model_version_id = ?
                """, [trigger["poll_id"], model_id]).fetchone()):
            raise ValueError("The triggering poll must refer to the selected model version")
        for site in sites:
            put_site(db, site)
        use_dates = job.get("window_params", "date" if block in CALENDAR_BLOCKS else None) == "date"
        trigger_run = instant(trigger["initialized_at"]) if trigger else None
        windows = [(start, end)] if full_response else split_windows(start, end, days, zone, use_dates)
        for batch_start, batch_end in windows:
            for batch_sites in batches(sites, site_size):
                for names in batches(list(all_fields), field_size):
                    fields = {field: dict(all_fields[field]) for field in names}
                    params = build_params(product, selector, job, raw_params, block, fields, batch_sites,
                                          batch_start, batch_end, zone, step, run)
                    profile = profile_data(params | {"timezone": timezone}, job.get("profile"),
                                           calendar=block in {"daily", "weekly", "monthly"})
                    spec = dict(job_name=name, product=product, sites=batch_sites, fields=fields, params=params,
                                profile=profile, selector=selector, selector_kind=job.get("selector_kind", "default"),
                                timezone=timezone, block=block, step=step, mode=mode, model_id=model_id,
                                full_response=full_response or (use_dates and block not in CALENDAR_BLOCKS))
                    if any(f.get("horizon_hours") for f in fields.values()):
                        spec["horizon_anchors"] = {basis: at.isoformat() for basis, at in
                                                   (("run", run), ("requested_window", start),
                                                    ("trigger_run", trigger_run)) if at is not None}
                    identity = [name, spec, batch_start, batch_end, run,
                                trigger.get("initialized_at") if trigger else anchor if block == "current" else None]
                    slice_id = uuid_key(identity)
                    ids.append(slice_id)
                    if db.execute("SELECT 1 FROM ops.extraction_slice WHERE slice_id = ?", [slice_id]).fetchone():
                        continue
                    live = mode == "live" or (mode == "exact" and trigger is not None)
                    units = round(max(1, len(batch_sites)) *
                                  max(1, len([f for f in fields.values() if f.get("support", "supported") == "supported"]) / 10) *
                                  max(1, (batch_end - batch_start).total_seconds() / 1209600,
                                      request_days / 14 if full_response else 0), 3)
                    insert(db, "ops.extraction_slice", [
                        slice_id, digest(identity), category, product["code"], selector, digest(batch_sites),
                        digest(profile), digest(names), batch_start, batch_end, anchor if live else None,
                        run, trigger.get("poll_id") if trigger else None, trigger.get("not_before") if trigger else None,
                        json_text(names), job.get("call_units", units), "queued", None, 0 if live else 10, json_text(spec)])
                    for api_field, rule in fields.items():
                        domain = job.get("domain", "global")
                        capability_id = key([product["code"], selector, domain, block, api_field, rule])
                        if not db.execute("SELECT 1 FROM ops.api_capability WHERE capability_id = ?",
                                          [capability_id]).fetchone():
                            parse_date = lambda value: datetime_at(value, UTC) if value else None
                            insert(db, "ops.api_capability", [capability_id, product["code"], selector, domain, block,
                                   api_field, json_text(rule.get("request_limits", {})), rule.get("horizon_hours"),
                                   rule.get("native_step_seconds"), rule.get("support", "supported"),
                                   parse_date(rule.get("verified_at")), parse_date(rule.get("effective_from")),
                                   parse_date(rule.get("effective_to")), json_text(rule)])
                        insert(db, "ops.slice_capability", [slice_id, capability_id])
                    planned = dict(spec, id=slice_id, start=batch_start, end=batch_end, run=run)
                    locations = batch_sites or [{"location_key": "__query__"}]
                    for site in locations:
                        for field, rule in (fields or {category: {}}).items():
                            for row in empty_coverage(planned, site["location_key"], field, rule, "queued", None):
                                insert(db, "silver.coverage", row)
    return ids


def batches(values, size):
    return [values[at:at + size] for at in range(0, len(values), size)] or [[]]


def split_windows(start, end, days, zone, calendar):
    while start < end:
        if calendar:
            limit = local_midnight(start.astimezone(zone).date() + timedelta(days=days), zone)
        else:
            limit = start + timedelta(days=days)
        stop = min(limit, end)
        yield start, stop
        start = stop


def build_params(product, selector, job, base, block, fields, sites, start, end, zone, step, run):
    params = dict(base)
    for coordinate in ("latitude", "longitude"):
        if sites:
            params[coordinate] = ",".join(str(site[coordinate]) for site in sites)
    if selector != NO_SELECTOR and product.get("selector_param", "models"):
        params[product.get("selector_param", "models")] = selector
    if run:
        params["run"] = run.strftime("%Y-%m-%dT%H:%M")
    if block == "result":
        return params
    params[block] = ",".join(name for name, rule in fields.items() if rule.get("support", "supported") == "supported")
    if product.get("timezone_param", True):
        params["timezone"] = zone.key
    if run or block == "current" or job.get("window_params") == "relative":
        return params
    style = job.get("window_params", block if block == "minutely_15" else "hour" if block == "hourly" else "date")
    if style == "date":
        params.update(start_date=start.astimezone(zone).date().isoformat(),
                      end_date=(end - timedelta(microseconds=1)).astimezone(zone).date().isoformat())
    elif style in {"hour", "minutely_15"} and step:
        params[f"start_{style}"] = start.astimezone(zone).strftime("%Y-%m-%dT%H:%M")
        params[f"end_{style}"] = (end - timedelta(seconds=step)).astimezone(zone).strftime("%Y-%m-%dT%H:%M")
    else:
        raise ValueError(f"Unsupported request window style {style}")
    return params


def get_slice(db, slice_id):
    row = db.execute("""
        SELECT slice_id, category, window_start, window_end, requested_run_at, poll_slot_at,
               estimated_call_units, status, specification, not_before_at, priority
        FROM ops.extraction_slice WHERE slice_id = ?
        """, [slice_id]).fetchone()
    if row is None:
        raise ValueError(f"Unknown slice {slice_id}")
    keys = ("id", "category", "start", "end", "run", "poll_slot", "units", "status", "spec", "not_before", "priority")
    result = dict(zip(keys, row))
    result.update(json.loads(result.pop("spec")))
    return result


def replay_mappings(slice_, config, job_name):
    job = resolve_job(config, job_name)
    if (job["product"], job.get("selector", NO_SELECTOR), job.get("block")) != (
            slice_["product"]["code"], slice_["selector"], slice_["block"]):
        raise ValueError("Replay mappings must use the original product, selector, and block")
    updated = choose_fields(config, job, now())
    plan_keys = {"support", "horizon_hours", "native_step_seconds", "verified_at",
                 "effective_from", "effective_to", "request_limits", "members", "member_kind",
                 "control_member", "horizon_reference"}
    fields = {}
    for name, old in slice_["fields"].items():
        candidate = updated.get(name, old)
        semantic = {k: v for k, v in candidate.items() if k not in plan_keys}
        prior = {k: v for k, v in old.items() if k not in plan_keys}
        if semantic != prior and candidate.get("mapping_version", 1) <= old.get("mapping_version", 1):
            raise ValueError(f"Raise the mapping version for {name} before replay")
        fields[name] = {**semantic, **{k: v for k, v in old.items() if k in plan_keys}}
    return dict(slice_, fields=fields)
