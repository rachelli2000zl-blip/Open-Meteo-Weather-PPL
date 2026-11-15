import hashlib
import json
import random
import re
import time
import tomllib
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory

import fcntl
import httpx


PROFILE = files("weather_pipeline").joinpath("berlin.toml").read_bytes()
CONFIG = tomllib.loads(PROFILE.decode())
CAPTURE_SECONDS = 60
MAX_ATTEMPTS = 3
MAX_BODY_BYTES = 2 * 1024 * 1024
RETRY_STATUS = {429, 500, 502, 503, 504}


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_bytes())


def atomic_json(path, value):
    with TemporaryDirectory(dir=path.parent) as temporary:
        staged = Path(temporary) / path.name
        staged.write_bytes(json_bytes(value))
        staged.replace(path)


@contextmanager
def writer_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError(f"Another pipeline is writing to {root}") from error
        yield


def windows(anchor):
    hour = anchor.replace(minute=0, second=0, microsecond=0)
    start = hour if anchor == hour else hour + timedelta(hours=1)
    return {"forecast_start_utc": start.isoformat(),
            "forecast_stop_utc": (start + timedelta(hours=CONFIG["forecast_hours"])).isoformat(),
            "archive_start": CONFIG["archive_start"].isoformat(),
            "archive_end": CONFIG["archive_end"].isoformat()}


def request_plan(capture_id, window):
    start = datetime.fromisoformat(window["forecast_start_utc"])
    stop = datetime.fromisoformat(window["forecast_stop_utc"])
    requests = []
    for product in ("forecast", "ensemble", "archive"):
        config = CONFIG[product]
        url, section, model = config["url"], config["section"], config["model"]
        params = {"latitude": CONFIG["latitude"], "longitude": CONFIG["longitude"],
                  "models": model, section: ",".join(config["fields"]),
                  "temperature_unit": "celsius"}
        if section == "hourly":
            # Open-Meteo's explicit end parameter denotes the last slot to return.
            end = stop - timedelta(hours=1)
            params.update(timezone="UTC", timeformat="unixtime",
                          start_hour=start.strftime("%Y-%m-%dT%H:%M"),
                          end_hour=end.strftime("%Y-%m-%dT%H:%M"))
        else:
            params.update(timezone=CONFIG["timezone"], timeformat="iso8601",
                          start_date=window["archive_start"], end_date=window["archive_end"])
        request_id = sha256(json_bytes([capture_id, url, params]))[:20]
        requests.append(dict(product=product, model=model, section=section, url=url, params=params,
                             request_id=request_id, fields=config["fields"]))
    return requests


def plan_capture(root, now=None):
    now = now or datetime.now(UTC)
    capture_id = now.strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    window = windows(now)
    capture = {"schema": 1, "capture_id": capture_id, "captured_at_utc": now.isoformat(),
               "site_id": CONFIG["site_id"], "timezone": CONFIG["timezone"], "window": window,
               "deadline_utc": (now + timedelta(seconds=CAPTURE_SECONDS)).isoformat(),
               "requests": request_plan(capture_id, window)}
    directory = root / "raw" / capture_id
    directory.mkdir(parents=True, exist_ok=False)
    atomic_json(directory / "capture.json", capture)
    return directory, capture


def attempts(directory, request):
    root = directory / "requests" / request["request_id"]
    return sorted(path for path in root.glob("*") if re.fullmatch(r"\d{3}", path.name))


def record_attempt(directory, request, number, meta, body):
    root = directory / "requests" / request["request_id"]
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root) as temporary:
        staged = Path(temporary) / "attempt"
        staged.mkdir()
        if body is not None:
            (staged / "response.json").write_bytes(body)
        meta = dict(meta, attempt=number, body_bytes=len(body) if body is not None else 0,
                    sha256=sha256(body) if body is not None else None)
        (staged / "meta.json").write_bytes(json_bytes(meta))
        staged.rename(root / f"{number:03}")


def retry_delay(header, number):
    if header:
        if header.isascii() and header.isdecimal() and len(header) <= 12:
            return int(header)
        try:
            retry_at = parsedate_to_datetime(header)
            if retry_at.tzinfo is not None:
                return max(0, (retry_at - datetime.now(UTC)).total_seconds())
        except (ValueError, OverflowError):
            pass
    return min(8, 2 ** (number - 1)) + random.uniform(0, 0.25)


def fetch_attempt(client, request, budget):
    started = time.monotonic()
    status, body, headers, error = None, None, {}, None
    complete, retryable = False, False
    try:
        with client.stream("GET", request["url"], params=request["params"],
                           timeout=min(8, budget)) as response:
            status = response.status_code
            headers = {key: response.headers[key] for key in
                       ("content-type", "content-encoding", "date", "retry-after")
                       if key in response.headers}
            body = bytearray()
            for chunk in response.iter_bytes():
                room = MAX_BODY_BYTES - len(body)
                body.extend(chunk[:room])
                if len(chunk) > room:
                    error = "response_too_large"
                    break
                if time.monotonic() - started >= budget:
                    error = "capture_deadline"
                    break
            else:
                complete = True
                retryable = status in RETRY_STATUS
    except httpx.TransportError as exc:
        error, retryable = type(exc).__name__, True
    except httpx.HTTPError as exc:
        error = type(exc).__name__
    meta = {"http_status": status, "complete": complete, "error": error,
            "fetched_at_utc": datetime.now(UTC).isoformat(), "headers": headers,
            "duration_ms": round((time.monotonic() - started) * 1000), "retryable": retryable}
    return meta, bytes(body) if body is not None else None


def fetch_capture(directory, capture):
    deadline = datetime.fromisoformat(capture["deadline_utc"])
    # Each Berlin request is small enough to fit one provider-equivalent call.
    # At most nine attempts can be made per capture, including resumed attempts.
    with httpx.Client(headers={"Accept": "application/json", "Accept-Encoding": "identity"}) as client:
        for request in capture["requests"]:
            committed = attempts(directory, request)
            previous = read_json(committed[-1] / "meta.json") if committed else None
            for number in range(len(committed) + 1, MAX_ATTEMPTS + 1):
                remaining = (deadline - datetime.now(UTC)).total_seconds()
                if previous:
                    if not previous["retryable"]:
                        break
                    wait_until = datetime.fromisoformat(previous["retry_at_utc"])
                    delay = max(0, (wait_until - datetime.now(UTC)).total_seconds())
                    if delay >= remaining:
                        break
                    time.sleep(delay)
                    remaining = (deadline - datetime.now(UTC)).total_seconds()
                if remaining <= 0:
                    break
                meta, body = fetch_attempt(client, request, remaining)
                if meta["retryable"]:
                    delay = retry_delay(meta["headers"].get("retry-after"), number)
                    meta["retry_at_utc"] = (datetime.now(UTC) + timedelta(seconds=delay)).isoformat()
                record_attempt(directory, request, number, meta, body)
                previous = meta
