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
Account-wide (or single-project) bird's-eye of every API key and its cost exposure.

Per project: billing + budget state, whether the Gemini API is enabled, and every API key
with its restriction class, Gemini deadline status, and real call volume. Flags the leak an
ad-hoc AI Studio key creates: a billable project with an unrestricted key and no budget.

GCP attributes no dollars to an individual key (billing export has no credential dimension),
so per-key cost here is call volume + services; project/SKU dollars need a BigQuery billing
export and are out of this read-only scan's scope. See ../../gcp-py/data/risk-reference.md.
"""

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Import the shared gcp-py library (sibling skill under .claude/skills/) - see gcp-py/SKILL.md.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "gcp-py", "scripts"))
import gcp
import keys
import cost

SCHEMA_VERSION = "1.0"
DEFAULT_WINDOW_DAYS = 90
GEMINI_API = "generativelanguage.googleapis.com"
SEV_ORDER = {"urgent": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def gemini_enabled(pid: str) -> Optional[bool]:
    """True/False if the Gemini API is enabled; None if the enabled-services list is unreadable."""
    r = gcp.run(["services", "list", "--enabled", f"--project={pid}",
                 "--format=value(config.name)"], parse_json=False, timeout=30)
    if r["outcome"] != gcp.OK:
        return None
    return GEMINI_API in (r["data"] or "")


def derive_findings(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    billable = rec["billing_enabled"] is True

    for k in rec["api_keys"]:
        u = k["usage"]
        live = u["status"] == "ok" and u["calls_in_window"] > 0
        if k["deadline_status"] == "breaks_2026_06_19":
            tail = f"; {u['calls_in_window']} calls/window (in use)" if live else ""
            out.append({"severity": "urgent", "kind": "unrestricted_key",
                        "detail": f"key '{k['name']}' is unrestricted - Gemini rejects it after "
                                  f"2026-06-19; restrict it or migrate to an auth key{tail}"})
        elif k["deadline_status"] == "restricted_until_2026_09":
            out.append({"severity": "medium", "kind": "standard_key",
                        "detail": f"key '{k['name']}' is a restricted standard key - migrate to "
                                  f"an auth key before 2026-09"})
        if u["status"] == "ok" and u["calls_in_window"] == 0:
            out.append({"severity": "low", "kind": "idle_key",
                        "detail": f"key '{k['name']}' unused in window - consider deleting"})

    if rec["keys_status"] not in (gcp.OK, gcp.DISABLED):
        out.append({"severity": "info", "kind": "data_gap",
                    "detail": f"API keys unreadable ({rec['keys_status']}) - cannot prove safe"})

    if rec["budget_status"] != gcp.OK:
        # A budget we cannot read on a BILLABLE project is a cost risk, not a footnote: we
        # cannot confirm a spend cap exists. Rank it above a plain unreadable footnote.
        sev = "medium" if billable else "info"
        tail = " on a billable project - cannot confirm a spend cap" if billable else ""
        out.append({"severity": sev, "kind": "data_gap",
                    "detail": f"billing/budget unreadable ({rec['budget_status']}){tail}"})
    elif billable and not rec["has_budget"]:
        unrestricted = any("unrestricted" in k["risk"] for k in rec["api_keys"])
        sev = "urgent" if unrestricted else "high"
        why = "unrestricted key + " if unrestricted else ""
        out.append({"severity": sev, "kind": "no_budget",
                    "detail": f"billable project with {why}NO budget - cap spend"})
    elif billable and not rec["has_project_budget"]:
        out.append({"severity": "medium", "kind": "only_account_budget",
                    "detail": "only an account-wide budget covers this project; add a project budget"})

    out.sort(key=lambda f: SEV_ORDER.get(f["severity"], 9))
    return out


def scan_project(pid: str, token: Optional[str], start: str, end: str) -> Dict[str, Any]:
    bud = cost.project_budgets(pid)
    kr = keys.list_keys(pid)
    api_keys: List[Dict[str, Any]] = []
    if kr["status"] == gcp.OK:
        for k in kr["keys"]:
            usage = keys.key_usage(pid, k.get("uid", ""), token, start, end)
            api_keys.append(keys.make_key_record(k, usage))
    rec = {
        "project_id": pid,
        "billing_enabled": bud["billing_enabled"],
        "billing_account": bud["billing_account"],
        "has_budget": bud["has_budget"],
        "has_project_budget": bud["has_project_budget"],
        "budgets": bud["budgets"],
        "budget_status": bud["status"],
        "gemini_enabled": gemini_enabled(pid),
        "keys_status": kr["status"],
        "api_keys": api_keys,
    }
    rec["findings"] = derive_findings(rec)
    return rec


def get_projects(only: Optional[str]) -> List[str]:
    if only:
        return [only]
    r = gcp.run(["projects", "list", "--format=value(projectId)"], parse_json=False, timeout=120)
    if r["outcome"] != gcp.OK:
        log(f"failed to list projects: {r['outcome']} {r['stderr'][:160]}")
        return []
    return [p.strip() for p in (r["data"] or "").splitlines() if p.strip()]


def assemble(records: List[Dict[str, Any]], account: str, window: int, scope: str) -> Dict[str, Any]:
    total_keys = sum(len(r["api_keys"]) for r in records)
    urgent = sum(1 for r in records for f in r["findings"] if f["severity"] == "urgent")
    unrestricted = sum(1 for r in records for k in r["api_keys"] if "unrestricted" in k["risk"])
    billable_no_budget = sum(1 for r in records
                             if r["billing_enabled"] is True and r["budget_status"] == gcp.OK
                             and not r["has_budget"])
    return {
        "schema_version": SCHEMA_VERSION,
        "scan": {"account": account, "scope": scope, "usage_window_days": window,
                 "generated_at": _iso(datetime.now(timezone.utc)),
                 "cost_note": "per-key cost is call volume + services; GCP exposes no per-key $."},
        "summary": {"projects": len(records), "api_keys": total_keys,
                    "urgent_findings": urgent, "unrestricted_keys": unrestricted,
                    "billable_no_budget": billable_no_budget},
        "projects": sorted(records, key=lambda r: min((SEV_ORDER.get(f["severity"], 9)
                                                       for f in r["findings"]), default=9)),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Bird's-eye of API keys + cost exposure across a GCP account.")
    ap.add_argument("--account", help="credentialed account to target (default: active gcloud account)")
    ap.add_argument("--quota-project", help="a project this account owns (for cross-org quota)")
    ap.add_argument("--project", help="single-project mode: scan only this project")
    ap.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--report", help="report path (default: reports/keys_report.<account>.json)")
    args = ap.parse_args()

    if args.account:
        gcp.set_account(args.account, args.quota_project)
    account = gcp.current_account()
    report_file = gcp.report_path(args.report, args.account, stem="keys_report")
    scope = "single" if args.project else "account-wide"
    log(f"account: {account or '(none)'} | scope: {scope} | report: {report_file.name}")

    token = gcp.access_token()
    now = datetime.now(timezone.utc)
    start, end = _iso(now - timedelta(days=args.window_days)), _iso(now)

    pids = get_projects(args.project)
    if not pids:
        log("No projects found for this account. Try: gcloud auth login")
        return
    log(f"scanning {len(pids)} project(s)...")

    records: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(scan_project, pid, token, start, end): pid for pid in pids}
        for fut in as_completed(futs):
            try:
                records.append(fut.result())
            except Exception as e:  # never let one project kill the run
                log(f"  ! error on {futs[fut]}: {e}")

    report = assemble(records, account, args.window_days, scope)
    report_file.write_text(json.dumps(report, indent=2, default=str))
    s = report["summary"]
    log("=" * 60)
    log(f"projects: {s['projects']} | keys: {s['api_keys']} | URGENT: {s['urgent_findings']} | "
        f"unrestricted keys: {s['unrestricted_keys']} | billable w/o budget: {s['billable_no_budget']}")
    log(f"report: {report_file}")


if __name__ == "__main__":
    main()
