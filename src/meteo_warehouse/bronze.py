import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import httpx

from .db import instant, insert, now, transaction, uuid_key
from .plan import get_slice, model_metadata, plan_job, public_url, resolve_job

BODY_LIMIT = 32 * 1024 * 1024
RETRYABLE = {408, 429, 500, 502, 503, 504}
LIMITS = ((60, 600), (3600, 5000), (86400, 10000), (2678400, 300000))


@dataclass(frozen=True)
class Response:
    status: int | None
    content_type: str | None
    body: bytes | None
    error: str | None
    fetched_at: datetime
    retry_after: str | None = None


def checksum(body):
    return hashlib.sha256(body).hexdigest() if body is not None else None


def checksum_matches(response):
    return response["body_sha256"] == checksum(response["response_body"])


def strict_json(body):
    def reject_constant(value):
        raise ValueError(f"Invalid JSON constant {value}")

    def unique_object(pairs):
        obj = dict(pairs)
        if len(pairs) != len(obj):
            raise ValueError("Duplicate JSON property")
        return obj

    return json.loads(body, parse_constant=reject_constant, object_pairs_hook=unique_object)


def fetch(client, url, params=None):
    status = content_type = body = retry_after = error = None
    try:
        with client.stream("GET", url, params=params) as response:
            status = response.status_code
            content_type = response.headers.get("content-type")
            retry_after = response.headers.get("retry-after")
            body = bytearray()
            for chunk in response.iter_bytes():
                remaining = BODY_LIMIT - len(body)
                body.extend(chunk[:remaining])
                if remaining < len(chunk):
                    error = "response_too_large"
                    break
    except httpx.HTTPError as exc:
        error = type(exc).__name__
    return Response(status, content_type, bytes(body) if body is not None else None,
                    error, now(), retry_after)


def available_budget(db, units, *, metadata=False):
    stamp = now()
    for seconds, limit in LIMITS:
        if metadata and seconds >= 86400:
            continue
        used = db.execute("""
            SELECT coalesce(sum(call_units), 0) FROM ops.call_reservation
            WHERE reserved_at > ? AND (? OR NOT is_metadata)
            """, [stamp - timedelta(seconds=seconds), seconds < 86400]).fetchone()[0]
        if used + units > limit:
            return False
    return True


def reserve(db, identifier, units, metadata=False):
    if available_budget(db, units, metadata=metadata):
        insert(db, "ops.call_reservation", [identifier, now(), units, metadata])
        return True
    return False


def land(db, slice_, response, *, requested_at, fetch_id=None, expected_sha256=None, evidence=None):
    from .db import json_text

    requested_at = instant(requested_at)
    fetched_at = instant(response.fetched_at)
    if fetched_at < requested_at:
        raise ValueError("Fetch time precedes request")
    if evidence and (evidence[0] > fetched_at or (slice_["run"] and evidence[0] < slice_["run"])):
        raise ValueError("Source availability falls outside the verified time bounds")
    fetch_id = fetch_id or uuid.uuid4()
    table = f"bronze.{slice_['category']}"
    source = public_url(slice_["product"]["url"])
    body_hash = expected_sha256 if expected_sha256 is not None else checksum(response.body)
    existing = db.execute(f"""
        SELECT slice_id, body_sha256, response_body, requested_at, fetched_at, http_status, fetch_error
        FROM {table} WHERE fetch_id = ?
    """, [fetch_id]).fetchone()
    if existing:
        if existing != (slice_["id"], body_hash, response.body, requested_at,
                        fetched_at, response.status, response.error):
            raise ValueError(f"Attempt {fetch_id} already contains different evidence")
        previous_evidence = db.execute("""
            SELECT source_available_at, source_reference, ordered_correction
            FROM ops.availability_evidence WHERE fetch_id = ?
        """, [fetch_id]).fetchone()
        if evidence and previous_evidence != evidence:
            raise ValueError("Attempt already has different availability evidence")
        return fetch_id
    with transaction(db):
        attempt = db.execute(f"SELECT coalesce(max(attempt_no), 0) + 1 FROM {table} WHERE slice_id = ?",
                             [slice_["id"]]).fetchone()[0]
        insert(db, table, [fetch_id, slice_["id"], attempt, slice_["product"]["code"],
               source.hostname, source.path, slice_["product"].get("provider"), slice_["selector"],
               slice_["selector_kind"], json_text(slice_["params"]), slice_["run"], requested_at, fetched_at,
               response.status, response.content_type, response.body, body_hash, response.error, slice_["units"]])
        if evidence:
            insert(db, "ops.availability_evidence", [fetch_id, *evidence])
        db.execute("UPDATE ops.extraction_slice SET status = 'landed' WHERE slice_id = ?", [slice_["id"]])
    return fetch_id


def read_attempt(db, slice_, fetch_id):
    table = f"bronze.{slice_['category']}"
    result = db.execute(f"SELECT * FROM {table} WHERE slice_id = ? AND fetch_id = ?", [slice_["id"], fetch_id])
    row = result.fetchone()
    if row is None:
        raise ValueError(f"Unknown attempt {fetch_id} for slice {slice_['id']}")
    return dict(zip([col[0] for col in result.description], row))


def retry_at(response, attempt_no):
    delay = min(60, 2 ** min(6, attempt_no - 1))
    if response.retry_after:
        try:
            header = response.retry_after.strip()
            if header.isascii() and header.isdecimal() and len(header) <= 8:
                delay = max(delay, int(header))
            else:
                when = parsedate_to_datetime(header).astimezone(UTC)
                delay = max(delay, (when - response.fetched_at).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    return response.fetched_at + timedelta(seconds=delay)


def capture(client, slice_):
    requested_at = now()
    return requested_at, fetch(client, slice_["product"]["url"], slice_["params"])


def work(db, client, *, max_requests=20, max_attempts=3, workers=4):
    from .silver import horizon_end, normalize, unavailable

    ids = db.execute("""
        SELECT slice_id FROM ops.extraction_slice
        WHERE status IN ('queued', 'deferred_quota', 'retryable', 'landed')
          AND (not_before_at IS NULL OR not_before_at <= ? OR status = 'landed')
        ORDER BY priority, window_start, slice_key
    """, [now()]).fetchall()
    results = []
    requests = 0
    pending = []

    def drain():
        completed = []
        starts = []
        usable = 0
        if not pending:
            return
        phase = "live" if pending[0][0]["priority"] == 0 else "backfill"
        for slice_, fetch_id, count, future in pending:
            requested_at, response = future.result()
            starts.append(requested_at)
            land(db, slice_, response, requested_at=requested_at, fetch_id=fetch_id)
            completed.append((slice_, fetch_id, count, response))
        pending.clear()
        for slice_, fetch_id, count, response in completed:
            status = normalize(db, slice_, fetch_id)
            usable += status in {"complete", "partial"}
            retryable = (response.status in RETRYABLE or
                         (response.error and response.error != "response_too_large"))
            if (retryable or status in {"partial", "invalid"}) and count + 1 < max_attempts:
                db.execute("UPDATE ops.extraction_slice SET status = 'retryable', not_before_at = ? WHERE slice_id = ?",
                           [retry_at(response, count + 1), slice_["id"]])
            results.append((slice_["id"], status))
        ids = [fetch_id for _, fetch_id, _, _ in completed]
        facts = db.execute("SELECT count(*) FROM silver.series_value WHERE fetch_id IN (SELECT unnest(?))",
                           [ids]).fetchone()[0]
        insert(db, "ops.ingestion_batch", [uuid.uuid4(), phase, min(starts), now(), len(ids), usable, facts])

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for (slice_id,) in ids:
            slice_ = get_slice(db, slice_id)
            if pending and slice_["priority"] != pending[0][0]["priority"]:
                drain()
            table = f"bronze.{slice_['category']}"
            if slice_["status"] == "landed":
                fetch_id = db.execute(f"""
                    SELECT fetch_id FROM {table} WHERE slice_id = ? ORDER BY attempt_no DESC LIMIT 1
                """, [slice_id]).fetchone()[0]
                results.append((slice_id, normalize(db, slice_, fetch_id)))
                continue
            names = list(slice_["fields"].values())
            if names and all(rule.get("support", "supported") != "supported" for rule in names):
                reason = "unverified" if any(rule.get("support") == "unverified" for rule in names) else "unsupported"
                results.append((slice_id, unavailable(db, slice_, reason)))
                continue
            if names and all(horizon_end(slice_, rule) <= slice_["start"] for rule in names
                             if rule.get("support", "supported") == "supported"):
                results.append((slice_id, unavailable(db, slice_, "outside_horizon")))
                continue
            if requests >= max_requests:
                continue
            count = db.execute(f"SELECT count(*) FROM {table} WHERE slice_id = ?", [slice_id]).fetchone()[0]
            if count >= max_attempts:
                db.execute("UPDATE ops.extraction_slice SET status='failed' WHERE slice_id=?", [slice_id])
                results.append((slice_id, "failed"))
                continue
            fetch_id = uuid.uuid4()
            if not reserve(db, fetch_id, slice_["units"]):
                results.append((slice_id, unavailable(db, slice_, "deferred_quota")))
                continue
            requests += 1
            pending.append((slice_, fetch_id, count, pool.submit(capture, client, slice_)))
            if len(pending) == workers:
                drain()
        drain()
    return results


def metadata_time(value):
    if type(value) is not int:
        raise ValueError("Invalid metadata timestamp")
    return datetime.fromtimestamp(value, UTC)


def poll(db, client, config, job_name):
    job = resolve_job(config, job_name)
    model = config.get("models", {}).get(job.get("model"))
    if not model or not model.get("metadata_url"):
        raise ValueError("The selected job requires a model with metadata_url")
    if job["mode"] not in {"live", "exact"}:
        raise ValueError("Model updates can trigger live or exact-run jobs")
    model_id = model_metadata(db, model)
    poll_id = uuid.uuid4()
    if not reserve(db, poll_id, 1, metadata=True):
        return "deferred_quota", []
    response = fetch(client, model["metadata_url"])
    error = response.error
    values = None
    if error is None and response.status == 200:
        try:
            payload = strict_json(response.body)
            initialized = metadata_time(payload["last_run_initialisation_time"])
            available = metadata_time(payload["last_run_availability_time"])
            mod = payload.get("last_run_modification_time")
            modified = metadata_time(mod) if mod is not None else None
            durations = [payload.get(name) for name in ("temporal_resolution_seconds", "update_interval_seconds")]
            if (initialized > available or any(x is not None and (type(x) is not int or x <= 0) for x in durations)):
                raise ValueError("Invalid metadata ordering or resolution")
            values = [poll_id, initialized, modified, available, *durations]
        except (ValueError, TypeError, KeyError, OverflowError, OSError):
            error = "invalid_model_metadata"
    with transaction(db):
        insert(db, "bronze.model_update", [poll_id, model_id, model["metadata_url"], response.fetched_at,
               response.status, response.body, checksum(response.body), error])
        if values:
            insert(db, "silver.model_update_observation", values)
    if values is None:
        return "invalid", []
    not_before = available + timedelta(minutes=10)
    existing = db.execute("""
        SELECT e.slice_id, e.specification FROM ops.extraction_slice e
        JOIN bronze.model_update p ON p.poll_id = e.trigger_poll_id
        JOIN silver.model_update_observation o ON o.poll_id = p.poll_id
        WHERE p.model_version_id = ? AND o.initialized_at = ?
        """, [model_id, initialized]).fetchall()
    matching = [slice_id for slice_id, spec in existing if json.loads(spec).get("job_name") == job_name]
    if matching:
        return "already_planned", matching
    trigger = dict(poll_id=poll_id, initialized_at=initialized, not_before=not_before)
    planned = plan_job(db, config, job_name, anchor=max(not_before, response.fetched_at),
                       run=initialized if job["mode"] == "exact" else None, trigger=trigger)
    return "planned", planned
