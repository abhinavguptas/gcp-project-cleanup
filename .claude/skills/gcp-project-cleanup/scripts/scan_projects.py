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
Decision-grade GCP project scanner.

Collects multiple independent signals per project (real API traffic, admin-activity
audit logs, resources + coverage, API keys WITH usage, service accounts, billing flag,
read access) and runs a multi-signal decision engine to recommend
keep / review / recycle_keys / delete. Every signal records known-vs-unknown, so a
project we could not read is never mistaken for an empty one.

Output: a single self-describing projects_report.json (schema v2.0). Long-running by
design; persists after every project and resumes by back-filling only the signals that
previously failed.
"""

import argparse
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import gcp

SCHEMA_VERSION = "2.0"

DEFAULT_USAGE_WINDOW_DAYS = 90   # Monitoring keeps 6 wks full-res, then downsamples (still queryable)
DEFAULT_AUDIT_WINDOW_DAYS = 400  # Admin Activity audit logs default retention
STALE_DAYS = 180                 # no resource metadata change for this long => candidate
ACTIVE_DAYS = 90                 # admin activity newer than this counts as "in use"

ASSET_TYPE_MAP = {
    "compute.googleapis.com/Instance": "instances",
    "compute.googleapis.com/Disk": "disks",
    "compute.googleapis.com/Snapshot": "snapshots",
    "compute.googleapis.com/Image": "images",
    "storage.googleapis.com/Bucket": "buckets",
    "sqladmin.googleapis.com/Instance": "sql_instances",
    "appengine.googleapis.com/Application": "app_engines",
    "appengine.googleapis.com/Version": "app_engines",
    "cloudfunctions.googleapis.com/CloudFunction": "cloud_functions",
    "run.googleapis.com/Service": "cloud_run",
}

# Signal statuses that are worth retrying on a resume (transient / fixable).
RETRYABLE = {"unknown", "error", "timeout"}

_print_lock = threading.Lock()


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    with _print_lock:
        print(f"[{ts}] {msg}", flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Signal collectors. Each returns a section dict carrying its own `status`.
# --------------------------------------------------------------------------- #

def collect_access(project: Dict[str, Any]) -> Dict[str, Any]:
    """Read access + parent + lifecycle. describe DENIED => we can't act on it."""
    pid = project["project_id"]
    r = gcp.run(["projects", "describe", pid], timeout=20)
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "parent": None,
                "lifecycle_state": project.get("lifecycle_state")}
    d = r["data"] or {}
    parent = d.get("parent")
    return {
        "status": "ok",
        "parent": ({"type": parent.get("type"), "id": parent.get("id")} if parent else None),
        "lifecycle_state": d.get("lifecycleState", project.get("lifecycle_state")),
    }


def collect_billing(pid: str, account_open: Dict[str, bool]) -> Dict[str, Any]:
    """Billing-enabled flag (no $ - GCP exposes no per-project cost API)."""
    r = gcp.run(["billing", "projects", "describe", pid], timeout=20)
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "enabled": None, "account": None, "account_open": None}
    d = r["data"] or {}
    acct = d.get("billingAccountName") or None
    return {
        "status": "ok",
        "enabled": bool(d.get("billingEnabled", False)),
        "account": acct,
        "account_open": account_open.get(acct) if acct else None,
    }


def collect_usage(pid: str, token: str, start: str, end: str) -> Dict[str, Any]:
    """Real consumer API traffic for the whole project."""
    if not token:
        return {"status": "unknown", "total_requests": 0, "last_request_time": None, "by_service": {}}
    f = ('metric.type="serviceruntime.googleapis.com/api/request_count"'
         ' AND resource.type="consumed_api"')
    m = gcp.monitoring_sum(pid, f, start, end, token)
    return {
        "status": m["status"],
        "total_requests": m["total"],
        "last_request_time": m["last_active"],
        "by_service": m["by_service"],
    }


def collect_activity(pid: str, audit_days: int) -> Dict[str, Any]:
    """Most recent meaningful Admin Activity audit log entry (is anyone touching it?).

    Excludes serviceusage EnableService/DisableService events: enabling APIs (e.g.
    the asset API needed to run this very scan) is housekeeping, not project use, and
    would otherwise make every project look freshly touched.
    """
    log_filter = ('log_id("cloudaudit.googleapis.com/activity")'
                  ' AND NOT protoPayload.serviceName="serviceusage.googleapis.com"')
    r = gcp.run([
        "logging", "read", log_filter,
        f"--project={pid}", "--limit=1", "--order=desc",
        f"--freshness={audit_days}d", "--format=json",
    ], timeout=90)
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "last_admin_action": None, "last_principal": None}
    entries = r["data"] or []
    if not entries:
        return {"status": "ok", "last_admin_action": None, "last_principal": None}
    e = entries[0]
    principal = (e.get("protoPayload", {}) or {}).get("authenticationInfo", {}).get("principalEmail")
    return {"status": "ok", "last_admin_action": e.get("timestamp"), "last_principal": principal}


def collect_resources(pid: str) -> Dict[str, Any]:
    """All resources via Asset Inventory, with an explicit coverage flag."""
    r = gcp.run(["asset", "search-all-resources", "--scope", f"projects/{pid}",
                 "--format=json"], timeout=60)
    if r["outcome"] != gcp.OK:
        # We could not enumerate -> we cannot prove the project is empty.
        return {"status": r["outcome"], "total": 0, "coverage_complete": False,
                "newest": None, "oldest": None, "counts_by_type": {}}
    assets = r["data"] or []
    counts: Dict[str, int] = {}
    stamps: List[datetime] = []
    for a in assets:
        cat = ASSET_TYPE_MAP.get(a.get("assetType", ""), "other")
        counts[cat] = counts.get(cat, 0) + 1
        for field in ("updateTime", "createTime"):
            dt = parse_ts(a.get(field, ""))
            if dt:
                stamps.append(dt)
    return {
        "status": "ok",
        "total": len(assets),
        "coverage_complete": True,
        "newest": iso_z(max(stamps)) if stamps else None,
        "oldest": iso_z(min(stamps)) if stamps else None,
        "counts_by_type": counts,
    }


def collect_credentials(pid: str, token: str, start: str, end: str) -> Dict[str, Any]:
    """API keys (each with real usage) + service accounts. Pre-stages key recycling."""
    keys_out: List[Dict[str, Any]] = []
    keys_status = "ok"
    rk = gcp.run(["services", "api-keys", "list", f"--project={pid}", "--format=json"], timeout=30)
    if rk["outcome"] != gcp.OK:
        keys_status = rk["outcome"]
    else:
        for k in (rk["data"] or []):
            uid = k.get("uid", "")
            restrictions = k.get("restrictions", {})
            usage = {"status": "unknown", "calls_in_window": 0, "last_used": None, "by_service": {}}
            if uid and token:
                # credential_id is a RESOURCE label (verified against live API),
                # value form "apikey:<uid>". The docs/blog that call it a metric
                # label are wrong for this metric.
                f = ('metric.type="serviceruntime.googleapis.com/api/request_count"'
                     ' AND resource.type="consumed_api"'
                     f' AND resource.labels.credential_id="apikey:{uid}"')
                m = gcp.monitoring_sum(pid, f, start, end, token)
                usage = {"status": m["status"], "calls_in_window": m["total"],
                         "last_used": m["last_active"], "by_service": m["by_service"]}
            keys_out.append({
                "name": k.get("displayName", k.get("name", "")),
                "uid": uid,
                "resource": k.get("name", ""),
                "created": k.get("createTime", ""),
                "restrictions": restrictions,
                "usage": usage,
                "risk": ([] if restrictions else ["unrestricted"]),
            })

    sas_out: List[Dict[str, Any]] = []
    sas_status = "ok"
    rs = gcp.run(["iam", "service-accounts", "list", f"--project={pid}", "--format=json"], timeout=30)
    if rs["outcome"] != gcp.OK:
        sas_status = rs["outcome"]
    else:
        for sa in (rs["data"] or []):
            email = sa.get("email", "")
            key_count, kc_status = None, "ok"
            rkeys = gcp.run(["iam", "service-accounts", "keys", "list",
                             f"--iam-account={email}", "--managed-by=user",
                             "--format=json"], timeout=20)
            if rkeys["outcome"] == gcp.OK:
                key_count = len(rkeys["data"] or [])
            else:
                kc_status = rkeys["outcome"]
            sas_out.append({"email": email, "disabled": bool(sa.get("disabled", False)),
                            "key_count": key_count, "status": kc_status})

    # Section status is the worse of the two list calls.
    status = "ok"
    for s in (keys_status, sas_status):
        if s != "ok":
            status = s
    return {"status": status, "api_keys": keys_out, "service_accounts": sas_out}


# --------------------------------------------------------------------------- #
# Decision engine
# --------------------------------------------------------------------------- #

def derive_signals(rec: Dict[str, Any]) -> Dict[str, Any]:
    usage, billing = rec["usage"], rec["billing"]
    res, creds, act = rec["resources"], rec["credentials"], rec["activity"]

    has_traffic = (usage["total_requests"] > 0) if usage["status"] == "ok" else None

    if creds["status"] != "ok":
        has_live_keys = None
    else:
        keys = creds["api_keys"]
        if not keys:
            has_live_keys = False
        elif any(k["usage"]["status"] == "ok" and k["usage"]["calls_in_window"] > 0 for k in keys):
            has_live_keys = True
        elif all(k["usage"]["status"] == "ok" for k in keys):
            has_live_keys = False
        else:
            has_live_keys = None  # keys exist but some usage unknown

    if billing["status"] != "ok":
        billing_active = None
    else:
        billing_active = bool(billing["enabled"]) and (billing["account_open"] is not False)

    if act["status"] != "ok":
        recent_admin = None
    elif not act["last_admin_action"]:
        recent_admin = False
    else:
        ts = parse_ts(act["last_admin_action"])
        recent_admin = bool(ts and (now_utc() - ts).days <= ACTIVE_DAYS)

    metadata_age_days = None
    if res["status"] == "ok" and res["total"] > 0 and res["newest"]:
        newest = parse_ts(res["newest"])
        if newest:
            metadata_age_days = (now_utc() - newest).days

    return {"has_traffic": has_traffic, "has_live_keys": has_live_keys,
            "billing_active": billing_active, "recent_admin_activity": recent_admin,
            "metadata_age_days": metadata_age_days}


def decide(rec: Dict[str, Any]) -> Dict[str, Any]:
    sig = rec["signals"]
    usage, res, creds, billing, act, access = (
        rec["usage"], rec["resources"], rec["credentials"], rec["billing"],
        rec["activity"], rec["access"])

    reasons: List[str] = []
    blockers: List[str] = []
    data_gaps: List[str] = []

    for name, sec in (("access", access), ("billing", billing), ("usage", usage),
                      ("activity", act), ("resources", res), ("credentials", creds)):
        if sec["status"] != "ok":
            data_gaps.append(f"{name}: {sec['status']}")

    # Keep-blockers are positive evidence of USE. Billing-enabled is deliberately NOT
    # one: an idle billing-linked project is the prime cleanup target (it costs money),
    # so billing is cost context (added to reasons below), never an auto-keep.
    if sig["has_traffic"]:
        blockers.append(f"live API traffic ({usage['total_requests']} req/window)")
    if sig["recent_admin_activity"]:
        blockers.append(f"admin activity {act.get('last_admin_action')} by {act.get('last_principal')}")

    cannot_prove_empty = (res["status"] != "ok" or not res["coverage_complete"]
                          or access["status"] == gcp.DENIED)

    if blockers:
        rec_name = "keep"
    elif cannot_prove_empty:
        rec_name = "review"
        reasons.append("cannot verify resources/access")
    else:
        empty = res["total"] == 0
        stale = sig["metadata_age_days"] is not None and sig["metadata_age_days"] > STALE_DAYS
        usage_zero = usage["status"] == "ok" and usage["total_requests"] == 0
        keys = creds["api_keys"]
        if empty or (stale and usage_zero):
            if empty:
                reasons.append("no resources")
            if stale:
                reasons.append(f"no resource change for {sig['metadata_age_days']}d")
            if sig["has_live_keys"] is None:
                rec_name = "review"
                reasons.append("API keys present, usage unknown")
            elif keys:  # idle keys present -> deletable, but recycle keys first
                rec_name = "recycle_keys"
                reasons.append(f"{len(keys)} idle API key(s) to recycle")
            else:
                rec_name = "delete"
        else:
            rec_name = "review"
            reasons.append("not clearly obsolete")

    if rec_name != "keep" and billing.get("enabled"):
        reasons.append("billing enabled (still billable)")

    # Confidence from how many decision-critical signals are unknown.
    critical = [usage["status"], res["status"], creds["status"]]
    unknownish = sum(1 for s in critical if s in RETRYABLE)
    if rec_name == "keep" and blockers:
        confidence = "high"
    elif unknownish == 0:
        confidence = "high"
    elif unknownish == 1:
        confidence = "medium"
    else:
        confidence = "low"

    return {"recommendation": rec_name, "confidence": confidence,
            "reasons": reasons, "blockers": blockers, "data_gaps": data_gaps}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

class Scanner:
    def __init__(self, workers: int, usage_window: int, audit_window: int,
                 only: Optional[str], report_file: Path):
        self.workers = workers
        self.usage_window = usage_window
        self.audit_window = audit_window
        self.only = only
        self.report_file = report_file
        self.start = iso_z(now_utc() - timedelta(days=usage_window))
        self.end = iso_z(now_utc())
        self.records: Dict[str, Dict[str, Any]] = {}
        self.account_open: Dict[str, bool] = {}
        self.account = gcp.current_account()  # resolved once, not on every save
        self._save_lock = threading.Lock()
        self._scan_started = iso_z(now_utc())

    # ---- persistence -------------------------------------------------------
    def load(self) -> None:
        if self.report_file.exists():
            try:
                data = json.loads(self.report_file.read_text())
                for r in data.get("projects", []):
                    self.records[r["project_id"]] = r
                log(f"Loaded {len(self.records)} existing project records (resume).")
            except (json.JSONDecodeError, OSError) as e:
                log(f"Could not load existing report: {e}")

    def _compute_coverage(self, projects: List[Dict[str, Any]]) -> Dict[str, str]:
        """Per signal: 'ok' if any project read it, else the most common failure."""
        sig_to_api = {"access": "resourcemanager", "billing": "billing", "usage": "monitoring",
                      "activity": "logging", "resources": "asset", "credentials": "apikeys/iam"}
        coverage: Dict[str, str] = {}
        for sig, api in sig_to_api.items():
            statuses = [r.get(sig, {}).get("status") for r in projects if sig in r]
            if not statuses:
                coverage[api] = "unknown"
            elif any(s == "ok" for s in statuses):
                coverage[api] = "ok"
            else:
                coverage[api] = max(set(statuses), key=statuses.count)
        return coverage

    def save(self, in_progress: bool = True) -> None:
        with self._save_lock:
            projects = list(self.records.values())
            by_rec = {"delete": 0, "recycle_keys": 0, "review": 0, "keep": 0}
            blocked = {"traffic": 0, "live_keys": 0, "recent_admin": 0}
            billable = 0
            gaps = 0
            for r in projects:
                d = r.get("decision", {})
                by_rec[d.get("recommendation", "review")] = by_rec.get(d.get("recommendation", "review"), 0) + 1
                if d.get("data_gaps"):
                    gaps += 1
                s = r.get("signals", {})
                if s.get("has_traffic"):
                    blocked["traffic"] += 1
                if s.get("has_live_keys"):
                    blocked["live_keys"] += 1
                if s.get("recent_admin_activity"):
                    blocked["recent_admin"] += 1
                if s.get("billing_active"):
                    billable += 1
            report = {
                "schema_version": SCHEMA_VERSION,
                "scan": {
                    "started_at": self._scan_started,
                    "finished_at": None if in_progress else iso_z(now_utc()),
                    "account": self.account,
                    "usage_window_days": self.usage_window,
                    "audit_window_days": self.audit_window,
                    "in_progress": in_progress,
                    "signal_coverage": self._compute_coverage(projects),
                },
                "summary": {"by_recommendation": by_rec, "kept_in_use_by": blocked,
                            "billable_projects": billable, "data_gaps": gaps,
                            "total": len(projects)},
                "projects": projects,
            }
            tmp = self.report_file.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(report, indent=2, default=str))
            tmp.replace(self.report_file)

    # ---- per-project work --------------------------------------------------
    def is_complete(self, pid: str) -> bool:
        """A record is complete if no signal failed in a retryable way."""
        r = self.records.get(pid)
        if not r:
            return False
        sections = [r.get(s, {}).get("status") for s in
                    ("access", "billing", "usage", "activity", "resources", "credentials")]
        return all(s not in RETRYABLE for s in sections)

    def scan_project(self, project: Dict[str, Any]) -> None:
        pid = project["project_id"]
        token = gcp.access_token()
        rec = self.records.get(pid, {})
        rec.update({
            "project_id": pid,
            "project_number": project.get("project_number"),
            "name": project.get("name"),
            "lifecycle_state": project.get("lifecycle_state"),
        })

        want = (lambda name: self.only is None or self.only == name)
        prov = rec.get("provenance", {})

        def stamp(name: str, status: str) -> None:
            prov[name] = {"ok": status == "ok", "status": status, "queried_at": iso_z(now_utc())}

        if want("access") or "access" not in rec:
            rec["access"] = collect_access(project)
            stamp("access", rec["access"]["status"])
        if want("billing") or "billing" not in rec:
            rec["billing"] = collect_billing(pid, self.account_open)
            stamp("billing", rec["billing"]["status"])
        if want("usage") or "usage" not in rec:
            rec["usage"] = collect_usage(pid, token, self.start, self.end)
            stamp("usage", rec["usage"]["status"])
        if want("activity") or "activity" not in rec:
            rec["activity"] = collect_activity(pid, self.audit_window)
            stamp("activity", rec["activity"]["status"])
        if want("resources") or "resources" not in rec:
            rec["resources"] = collect_resources(pid)
            stamp("resources", rec["resources"]["status"])
        if want("credentials") or "credentials" not in rec:
            rec["credentials"] = collect_credentials(pid, token, self.start, self.end)
            stamp("credentials", rec["credentials"]["status"])

        rec["parent"] = rec["access"].get("parent")
        rec["provenance"] = prov
        rec["signals"] = derive_signals(rec)
        rec["decision"] = decide(rec)
        self.records[pid] = rec
        self.save(in_progress=True)
        d = rec["decision"]
        log(f"  {pid:42s} -> {d['recommendation']:12s} ({d['confidence']}) "
            f"{'; '.join(d['blockers'][:1])}")

    # ---- driver ------------------------------------------------------------
    def run(self, projects: List[Dict[str, Any]], fresh: bool) -> None:
        self._load_billing_accounts()
        if fresh or self.only:
            pending = projects  # --only re-collects one signal across every project
        else:
            pending = [p for p in projects if not self.is_complete(p["project_id"])]
        log(f"{len(projects)} projects total; {len(pending)} to scan "
            f"({len(projects) - len(pending)} already complete).")

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futs = {pool.submit(self.scan_project, p): p for p in pending}
            done = 0
            for fut in as_completed(futs):
                done += 1
                try:
                    fut.result()
                except Exception as e:  # never let one project kill the run
                    log(f"  ! error on {futs[fut]['project_id']}: {e}")
                if done % 10 == 0:
                    log(f"progress: {done}/{len(pending)}")

        self.save(in_progress=False)
        self._print_summary()

    def _load_billing_accounts(self) -> None:
        r = gcp.run(["billing", "accounts", "list", "--format=json"], timeout=30)
        if r["outcome"] == gcp.OK:
            for a in (r["data"] or []):
                self.account_open[a.get("name")] = bool(a.get("open", False))

    def _print_summary(self) -> None:
        report = json.loads(self.report_file.read_text())
        s = report["summary"]
        log("=" * 64)
        log(f"Scanned: {s['total']}  |  data gaps: {s['data_gaps']}")
        log(f"  delete       : {s['by_recommendation']['delete']}")
        log(f"  recycle_keys : {s['by_recommendation']['recycle_keys']}")
        log(f"  review       : {s['by_recommendation']['review']}")
        log(f"  keep         : {s['by_recommendation']['keep']}")
        log(f"Report: {self.report_file}")


def get_all_projects() -> List[Dict[str, Any]]:
    r = gcp.run(["projects", "list", "--format=json"], timeout=120)
    if r["outcome"] != gcp.OK:
        log(f"Failed to list projects: {r['outcome']} {r['stderr'][:160]}")
        return []
    out = []
    for p in (r["data"] or []):
        out.append({
            "project_id": p.get("projectId"),
            "name": p.get("name"),
            "project_number": str(p.get("projectNumber", "")),
            "lifecycle_state": p.get("lifecycleState"),
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Decision-grade GCP project scanner")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=None, help="scan only first N projects")
    ap.add_argument("--window-days", type=int, default=DEFAULT_USAGE_WINDOW_DAYS,
                    help=f"usage lookback in days (default {DEFAULT_USAGE_WINDOW_DAYS})")
    ap.add_argument("--audit-days", type=int, default=DEFAULT_AUDIT_WINDOW_DAYS)
    ap.add_argument("--only", choices=["access", "billing", "usage", "activity",
                                       "resources", "credentials"],
                    help="re-collect just one signal for every project")
    ap.add_argument("--account", help="target a credentialed account (default: active gcloud account)")
    ap.add_argument("--quota-project",
                    help="project this account owns, for API quota (needed when --account is "
                         "not the active account; else resource scans get denied)")
    ap.add_argument("--report", help="report file path (default: projects_report.<account>.json)")
    ap.add_argument("--fresh", action="store_true", help="ignore existing report, rescan all")
    args = ap.parse_args()

    if args.account:
        gcp.set_account(args.account, args.quota_project)
    report_file = gcp.report_path(args.report, args.account)
    log(f"account: {gcp.current_account() or '(none)'} | report: {report_file.name}")

    scanner = Scanner(args.workers, args.window_days, args.audit_days, args.only, report_file)
    if not args.fresh:
        scanner.load()

    projects = get_all_projects()
    if args.limit:
        projects = projects[:args.limit]
    if not projects:
        log("No projects found for this account. Try: gcloud auth login")
        return

    scanner.run(projects, fresh=args.fresh)


if __name__ == "__main__":
    main()
