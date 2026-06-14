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
Delete GCP projects recommended for deletion by scan_projects.py.

Reads projects_report.json (schema v2.0) and deletes only projects whose decision is
`delete`. Before deleting ANY project it performs a LIVE API-key re-check - not a read
of the (possibly stale) report - so a project that has grown a live API key since the
scan can never be deleted out from under a running integration:

  * a key with traffic in the window -> HARD BLOCK (even with --allow-keyed)
  * idle / unverifiable keys          -> BLOCK unless --allow-keyed
  * key listing unreadable            -> BLOCK (cannot prove safety)

WARNING: project deletion is irreversible after the ~30-day recovery window.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import gcp

DEFAULT_WINDOW_DAYS = 90


def log(msg: str, level: str = "INFO") -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {level}: {msg}", flush=True)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def live_key_guard(pid: str, window_days: int, allow_keyed: bool) -> Tuple[bool, str]:
    """Re-check API keys live. Returns (blocked, reason)."""
    r = gcp.run(["services", "api-keys", "list", f"--project={pid}", "--format=json"], timeout=30)
    if r["outcome"] != gcp.OK:
        return True, f"cannot verify API keys ({r['outcome']})"
    keys = r["data"] or []
    if not keys:
        return False, "no API keys"

    token = gcp.access_token()
    start, end = _iso(datetime.now(timezone.utc) - timedelta(days=window_days)), _iso(datetime.now(timezone.utc))
    live, unverifiable = [], 0
    for k in keys:
        uid = k.get("uid", "")
        name = k.get("displayName", uid)
        if not uid:
            unverifiable += 1
            continue
        f = ('metric.type="serviceruntime.googleapis.com/api/request_count"'
             ' AND resource.type="consumed_api"'
             f' AND resource.labels.credential_id="apikey:{uid}"')
        m = gcp.monitoring_sum(pid, f, start, end, token)
        if m["status"] == "ok" and m["total"] > 0:
            live.append(f"{name}={m['total']}")
        elif m["status"] != "ok":
            unverifiable += 1

    if live:  # hard block - cannot be overridden
        return True, f"LIVE API key traffic: {', '.join(live)}"
    if unverifiable and not allow_keyed:
        return True, f"{unverifiable} key(s) with unverifiable usage (use --allow-keyed)"
    if not allow_keyed:
        return True, f"{len(keys)} idle API key(s) present - recycle first or --allow-keyed"
    return False, f"{len(keys)} idle key(s), allowed by --allow-keyed"


class Deleter:
    def __init__(self, report_file: Path, execute: bool, include_recycle: bool,
                 allow_keyed: bool, window_days: int, assume_yes: bool = False):
        self.report_file = report_file
        self.execute = execute
        self.include_recycle = include_recycle
        self.allow_keyed = allow_keyed
        self.window_days = window_days
        self.assume_yes = assume_yes  # skip interactive prompt (skill/background use)
        self.data: Dict[str, Any] = {}
        self.deleted: List[str] = []
        self.blocked: List[Tuple[str, str]] = []
        self.failed: List[Tuple[str, str]] = []

    def load(self, only_ids: List[str] = None) -> List[Dict[str, Any]]:
        if not self.report_file.exists():
            log(f"Report not found: {self.report_file}. Run scan_projects.py first.", "ERROR")
            sys.exit(1)
        self.data = json.loads(self.report_file.read_text())
        projects = self.data.get("projects", [])
        if only_ids:  # explicit worklist (e.g. from triage) overrides recommendations
            by_id = {p.get("project_id"): p for p in projects}
            candidates = []
            for pid in only_ids:
                p = by_id.get(pid, {"project_id": pid, "decision": {"recommendation": "(manual)"}})
                if p.get("deletion_status") != "deleted":
                    candidates.append(p)
            log(f"explicit worklist: {len(candidates)} project(s)")
            return candidates
        wanted = {"delete"} | ({"recycle_keys"} if self.include_recycle else set())
        candidates = [p for p in projects
                      if p.get("decision", {}).get("recommendation") in wanted
                      and p.get("deletion_status") != "deleted"]
        log(f"Report: {self.data.get('schema_version')} | candidates ({'/'.join(sorted(wanted))}): {len(candidates)}")
        return candidates

    def _save(self) -> None:
        try:
            self.report_file.write_text(json.dumps(self.data, indent=2, default=str))
        except OSError as e:
            log(f"could not persist report: {e}", "WARN")

    def _mark_deleted(self, pid: str) -> None:
        for p in self.data.get("projects", []):
            if p.get("project_id") == pid:
                p["deletion_status"] = "deleted"
                p["deleted_at"] = datetime.now().isoformat()
                break
        self._save()

    def process(self, candidates: List[Dict[str, Any]]) -> None:
        mode = "EXECUTE" if self.execute else "DRY RUN"
        log("=" * 64)
        log(f"PROJECT DELETION - {mode}")
        log("=" * 64)
        if not candidates:
            log("Nothing recommended for deletion. (This is a safe outcome.)")
            return

        # Confirm once, up front, before any irreversible action.
        if self.execute and not self.assume_yes:
            log(f"About to delete up to {len(candidates)} project(s) after live key checks.", "WARN")
            if input("Type 'DELETE' to confirm: ") != "DELETE":
                log("Cancelled.")
                return

        for i, p in enumerate(candidates, 1):
            pid = p["project_id"]
            rec = p.get("decision", {}).get("recommendation")
            log(f"[{i}/{len(candidates)}] {pid} ({rec})")
            blocked, reason = live_key_guard(pid, self.window_days, self.allow_keyed)
            if blocked:
                log(f"  BLOCKED: {reason}", "WARN")
                self.blocked.append((pid, reason))
                continue
            log(f"  guard ok: {reason}")
            if not self.execute:
                log("  [DRY RUN] would delete", "OK")
                continue
            r = gcp.run(["projects", "delete", pid, "--quiet"], parse_json=False, timeout=60)
            if r["outcome"] == gcp.OK:
                log("  deletion initiated", "OK")
                self.deleted.append(pid)
                self._mark_deleted(pid)
                time.sleep(1)
            else:
                log(f"  FAILED: {r['stderr'][:160]}", "ERROR")
                self.failed.append((pid, r["stderr"][:160]))

        self._summary()

    def _summary(self) -> None:
        log("=" * 64)
        log(f"{'deleted' if self.execute else 'would delete'}: {len(self.deleted)}  "
            f"blocked: {len(self.blocked)}  failed: {len(self.failed)}")
        for pid, reason in self.blocked:
            log(f"  blocked {pid}: {reason}")
        for pid, reason in self.failed:
            log(f"  failed  {pid}: {reason}", "ERROR")


def main() -> None:
    ap = argparse.ArgumentParser(description="Delete GCP projects from a scan report")
    ap.add_argument("--file", type=Path, default=None,
                    help="report path (default: projects_report.<account>.json)")
    ap.add_argument("--account", help="target account (default: active gcloud account)")
    ap.add_argument("--quota-project",
                    help="project this account owns, for API quota (needed with a non-active --account)")
    ap.add_argument("--projects",
                    help="comma-separated project IDs to delete (overrides report recommendations)")
    ap.add_argument("--execute", action="store_true", help="actually delete (default: dry run)")
    ap.add_argument("--include-recycle", action="store_true",
                    help="also process 'recycle_keys' projects")
    ap.add_argument("--allow-keyed", action="store_true",
                    help="permit deletion of projects with idle keys (never overrides live traffic)")
    ap.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    ap.add_argument("--yes", action="store_true",
                    help="skip the interactive DELETE prompt (for skill/background use)")
    args = ap.parse_args()

    if args.account:
        gcp.set_account(args.account, args.quota_project)
    report_file = gcp.report_path(str(args.file) if args.file else None, args.account)
    only_ids = [p.strip() for p in args.projects.split(",") if p.strip()] if args.projects else None

    deleter = Deleter(report_file, args.execute, args.include_recycle, args.allow_keyed,
                      args.window_days, assume_yes=args.yes)
    deleter.process(deleter.load(only_ids))


if __name__ == "__main__":
    main()
