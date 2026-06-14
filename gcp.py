#!/usr/bin/env python3
# Copyright 2026 Concret.io
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Shared gcloud / Google Cloud REST helpers.

The single most important guarantee here: a call that FAILED (permission denied,
API disabled, timeout, transport error) is never reported as if it SUCCEEDED WITH
NO DATA. Every helper returns an explicit `outcome`, so callers can tell
"read it, found nothing" apart from "could not read it" - the distinction the old
code lost and that made permission-denied projects look empty (and deletable).
"""

import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Call outcomes. `ok` means the command ran and returned data (which may legitimately
# be an empty list). Everything else means we did NOT get a trustworthy answer.
OK = "ok"
DENIED = "denied"        # caller lacks permission (403 / PERMISSION_DENIED)
DISABLED = "disabled"    # the API is not enabled on the project
TIMEOUT = "timeout"
ERROR = "error"

_token_lock = threading.Lock()
_token_cache: Dict[str, float] = {"value": "", "fetched_at": 0.0}

# Optional account override. When set, every gcloud call and token fetch targets this
# account (and a quota project it owns) via per-subprocess env vars, WITHOUT mutating the
# user's active gcloud config. The quota project matters: serviceusage-gated APIs (e.g.
# Cloud Asset) attribute quota to core/project, so a cross-org active project => denied.
_account_override: Optional[str] = None
_quota_project: Optional[str] = None


def _env() -> Dict[str, str]:
    e = dict(os.environ)
    if _account_override:
        e["CLOUDSDK_CORE_ACCOUNT"] = _account_override
        if _quota_project:
            e["CLOUDSDK_CORE_PROJECT"] = _quota_project
    return e


def set_account(email: Optional[str], quota_project: Optional[str] = None) -> None:
    """Target a credentialed account for all subsequent calls (no config change).

    serviceusage-gated APIs (e.g. Cloud Asset) attribute quota to core/project. When
    targeting a NON-active account, the active config project belongs to another org and
    the account can't use it -> pass `quota_project` (a project this account owns). When
    targeting the active account, the coherent active project is used automatically.
    """
    global _account_override, _quota_project
    _account_override = email or None
    _quota_project = quota_project or None
    with _token_lock:  # force token refresh for the new account
        _token_cache["value"] = ""
        _token_cache["fetched_at"] = 0.0


def current_account() -> str:
    """The account in effect: the override if set, else gcloud's active account."""
    if _account_override:
        return _account_override
    r = run(["config", "get-value", "account"], parse_json=False, timeout=10)
    return (r["data"] or "").strip() if r["outcome"] == OK else ""


def account_slug(email: Optional[str]) -> str:
    """Filename-safe slug for an account email, e.g. a@b.cloud -> a-b-cloud."""
    return re.sub(r"[^a-z0-9]+", "-", (email or "default").lower()).strip("-") or "default"


def report_path(override: Optional[str] = None, account: Optional[str] = None) -> Path:
    """Per-account report path, or `override` verbatim if given."""
    if override:
        return Path(override)
    return Path(__file__).parent / f"projects_report.{account_slug(account or current_account())}.json"


def classify_stderr(stderr: str) -> str:
    """Map a gcloud stderr blob to a failure outcome."""
    s = (stderr or "").lower()
    # Check "API not enabled" FIRST: GCP's disabled-API errors also contain
    # "does not have permission", which would otherwise mask them as DENIED.
    if "has not been used" in s or "is disabled" in s or "service_disabled" in s \
            or "accessnotconfigured" in s or "enable it by visiting" in s:
        return DISABLED
    if "permission" in s or "403" in s or "does not have" in s or "forbidden" in s:
        return DENIED
    return ERROR


def run(args: List[str], timeout: int = 30, parse_json: bool = True) -> Dict[str, Any]:
    """Run a gcloud command.

    Returns: {outcome, data, stderr}. `data` is parsed JSON on success (or the raw
    string when parse_json=False), and None on any failure.
    """
    command = ["gcloud"] + args
    if parse_json and not any(a == "--format" or a.startswith("--format=") for a in args):
        command += ["--format=json"]

    try:
        proc = subprocess.run(command, capture_output=True, text=True,
                              check=False, timeout=timeout, env=_env())
    except subprocess.TimeoutExpired:
        return {"outcome": TIMEOUT, "data": None, "stderr": f"timeout after {timeout}s"}
    except Exception as e:  # transport / spawn failure
        return {"outcome": ERROR, "data": None, "stderr": str(e)}

    if proc.returncode != 0:
        return {"outcome": classify_stderr(proc.stderr), "data": None,
                "stderr": (proc.stderr or "").strip()}

    if not parse_json:
        return {"outcome": OK, "data": proc.stdout, "stderr": ""}

    out = proc.stdout.strip()
    if not out:
        return {"outcome": OK, "data": [], "stderr": ""}
    try:
        return {"outcome": OK, "data": json.loads(out), "stderr": ""}
    except json.JSONDecodeError as e:
        return {"outcome": ERROR, "data": None, "stderr": f"json parse: {e}"}


def access_token(max_age: int = 1800) -> Optional[str]:
    """Return a cached gcloud access token, refreshing every `max_age` seconds."""
    with _token_lock:
        if _token_cache["value"] and (time.time() - _token_cache["fetched_at"]) < max_age:
            return _token_cache["value"]
        try:
            proc = subprocess.run(["gcloud", "auth", "print-access-token"],
                                  capture_output=True, text=True, timeout=15, env=_env())
        except Exception:
            return _token_cache["value"] or None
        if proc.returncode == 0 and proc.stdout.strip():
            _token_cache["value"] = proc.stdout.strip()
            _token_cache["fetched_at"] = time.time()
            return _token_cache["value"]
        return _token_cache["value"] or None


def monitoring_sum(project_id: str, metric_filter: str, start: str, end: str,
                   token: str, timeout: int = 30) -> Dict[str, Any]:
    """Sum serviceruntime request_count time series over a window.

    Uses per-series daily ALIGN_SUM (no cross-series reduce, so the brittle
    aggregation.groupByFields syntax is avoided) and totals client-side, grouping
    by each series' `service` resource label. Daily points give day-resolution
    "last used".

    The HTTP call goes through `curl`, not urllib, on purpose: the macOS python.org
    build ships no CA bundle, so urllib fails cert verification while curl (like
    gcloud itself) uses the system trust store. Keeps the tool free of Python deps.

    Returns: {status: "ok"|"unknown", total: int, by_service: {..}, last_active: iso|None}.
    `status: "ok"` with `total: 0` means a CONFIRMED zero. `status: "unknown"` means
    the query could not be answered - callers must NOT treat it as zero.
    """
    unknown = {"status": "unknown", "total": 0, "by_service": {}, "last_active": None}
    if not token:
        return unknown
    url = f"https://monitoring.googleapis.com/v3/projects/{project_id}/timeSeries"
    cmd = [
        "curl", "-s", "--max-time", str(timeout), "-G", url,
        "-H", f"Authorization: Bearer {token}",
        "--data-urlencode", f"filter={metric_filter}",
        "--data-urlencode", f"interval.startTime={start}",
        "--data-urlencode", f"interval.endTime={end}",
        "--data-urlencode", "aggregation.alignmentPeriod=86400s",
        "--data-urlencode", "aggregation.perSeriesAligner=ALIGN_SUM",
        "--data-urlencode", "view=FULL",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    except Exception:
        return unknown
    if proc.returncode != 0 or not proc.stdout.strip():
        return unknown
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return unknown
    if isinstance(data, dict) and data.get("error"):
        # Classify so callers can tell a terminal "can't read" (no point retrying)
        # from a transient failure - and never from a confirmed zero.
        err = data["error"]
        msg = (err.get("message", "") or "").lower()
        if "has not been used" in msg or "is disabled" in msg or "is not enabled" in msg:
            return {**unknown, "status": DISABLED}
        if err.get("code") == 403 or err.get("status") == "PERMISSION_DENIED":
            return {**unknown, "status": DENIED}
        return unknown

    by_service: Dict[str, int] = {}
    last_active: Optional[str] = None
    for ts in data.get("timeSeries", []):
        service = ts.get("resource", {}).get("labels", {}).get("service", "unknown")
        for pt in ts.get("points", []):
            val = int(pt.get("value", {}).get("int64Value", 0) or 0)
            if val <= 0:
                continue
            by_service[service] = by_service.get(service, 0) + val
            end_time = pt.get("interval", {}).get("endTime")
            if end_time and (last_active is None or end_time > last_active):
                last_active = end_time

    return {"status": "ok", "total": sum(by_service.values()),
            "by_service": by_service, "last_active": last_active}
