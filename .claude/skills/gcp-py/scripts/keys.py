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
API-key and service-account-key primitives - the one place key logic lives.

Pure functions over gcloud + Cloud Monitoring. No policy, no flow: this module reports
only what GCP exposes; callers decide what a finding means. Every function returns an
explicit status so "read it, no keys" is never confused with "could not read it".

Import via the gcp-py shim (see SKILL.md); requires sibling `gcp` on sys.path.
"""

from typing import Any, Dict, List, Optional

import gcp

# APIs whose access through an unrestricted key is the costly exposure.
GEMINI_APIS = {"generativelanguage.googleapis.com"}


def list_keys(project: str) -> Dict[str, Any]:
    """Raw API keys for a project: {status, keys, note}.

    Returns the TRUE outcome (does not collapse DISABLED to ok) so each caller chooses its
    own policy: a posture scan may treat DISABLED as "API off => no keys can exist => safe",
    while the deletion guard treats anything other than ok as "cannot verify => block".
    """
    r = gcp.run(["services", "api-keys", "list", f"--project={project}", "--format=json"], timeout=30)
    if r["outcome"] == gcp.DISABLED:
        return {"status": gcp.DISABLED, "keys": [], "note": "apikeys API disabled"}
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "keys": [], "note": r["stderr"]}
    return {"status": gcp.OK, "keys": r["data"] or [], "note": ""}


def classify_key(k: Dict[str, Any]) -> Dict[str, Any]:
    """Restriction state + Gemini deadline status from a raw key dict.

    api_restricted: has an `apiTargets` restriction - this is exactly what the 2026-06-19
      cutoff checks (unrestricted standard keys get rejected).
    app_restricted: has a browser/server/android/ios restriction - a separate hardening axis.
    deadline_status reflects restriction state only; gcloud does not expose standard-vs-auth
      key type, so a key shown as `restricted_until_2026_09` may in fact be a safe auth key.
    """
    restr = k.get("restrictions", {}) or {}
    api_restricted = bool(restr.get("apiTargets"))
    app_restricted = any(restr.get(x) for x in (
        "browserKeyRestrictions", "serverKeyRestrictions",
        "androidKeyRestrictions", "iosKeyRestrictions"))
    deadline = "breaks_2026_06_19" if not api_restricted else "restricted_until_2026_09"
    return {"api_restricted": api_restricted, "app_restricted": app_restricted,
            "deadline_status": deadline}


def key_usage(project: str, uid: str, token: Optional[str], start: str, end: str) -> Dict[str, Any]:
    """Per-key request volume in [start, end]: {status, calls_in_window, last_used, by_service}.

    `credential_id` is a RESOURCE label of form "apikey:<uid>", verified against the live API;
    docs/blogs that call it a metric label are wrong for this metric. status "ok" with 0 calls
    is a CONFIRMED zero; "unknown" means callers must not treat it as zero.
    """
    blank = {"status": "unknown", "calls_in_window": 0, "last_used": None, "by_service": {}}
    if not uid or not token:
        return blank
    f = f'{gcp.REQUEST_COUNT_FILTER} AND resource.labels.credential_id="apikey:{uid}"'
    m = gcp.monitoring_sum(project, f, start, end, token)
    return {"status": m["status"], "calls_in_window": m["total"],
            "last_used": m["last_active"], "by_service": m["by_service"]}


def make_key_record(k: Dict[str, Any], usage: Dict[str, Any]) -> Dict[str, Any]:
    """Canonical per-key record - the shape the gcp-* report schemas reference."""
    c = classify_key(k)
    risk: List[str] = []
    if not c["api_restricted"] and not c["app_restricted"]:
        risk.append("unrestricted")
    return {
        "name": k.get("displayName") or k.get("name", ""),
        "uid": k.get("uid", ""),
        "resource": k.get("name", ""),
        "created": k.get("createTime", ""),
        "restrictions": k.get("restrictions", {}) or {},
        "api_restricted": c["api_restricted"],
        "app_restricted": c["app_restricted"],
        "deadline_status": c["deadline_status"],
        "usage": usage,
        "risk": risk,
    }


def sa_user_keys(project: str) -> Dict[str, Any]:
    """User-managed (downloadable) service-account keys - the exfil/sprawl risk. Google-managed
    keys rotate automatically and are not reported.

    Returns {status, service_accounts:[{email, disabled, user_key_count, status}]}. The top-level
    status is the SA-list call outcome; a per-SA key-list failure is recorded in that SA's own
    `status` with user_key_count=None, so it is never silently counted as zero keys.
    """
    r = gcp.run(["iam", "service-accounts", "list", f"--project={project}", "--format=json"], timeout=30)
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "service_accounts": []}
    out: List[Dict[str, Any]] = []
    for sa in (r["data"] or []):
        email = sa.get("email", "")
        kc: Optional[int] = None
        kr = gcp.run(["iam", "service-accounts", "keys", "list",
                      f"--iam-account={email}", "--managed-by=user", "--format=json"], timeout=20)
        if kr["outcome"] == gcp.OK:
            kc = len(kr["data"] or [])
        out.append({"email": email, "disabled": bool(sa.get("disabled", False)),
                    "user_key_count": kc, "status": kr["outcome"]})
    return {"status": gcp.OK, "service_accounts": out}
