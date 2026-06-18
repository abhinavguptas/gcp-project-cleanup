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
Check and self-grant the IAM roles needed by the gcp-* skills.

Tests the active account at project, org, and billing scopes. Reports missing
permissions per skill and, with --fix (or --dry-run), emits the grant commands.

Usage:
  python3 check_perms.py --project PROJECT_ID [--fix | --dry-run] [--json]
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Set, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "gcp-py", "scripts"))
import gcp
from gcp import OK

# ── Required permission sets ──────────────────────────────────────────────────

# Project-scoped: guardrails + api-cost-guard (per-project audit)
PROJECT_PERMS = [
    "resourcemanager.projects.getIamPolicy",
    "serviceusage.services.list",
    "iam.serviceAccounts.list",
    "iam.serviceAccountKeys.list",
    "apikeys.keys.list",
    "monitoring.timeSeries.list",
    "logging.logEntries.list",
    "run.services.list",
    "compute.instances.list",
    "sqladmin.instances.list",
    "orgpolicy.policies.list",
]

# Org-scoped: project-cleanup (cross-org scan)
ORG_PERMS = [
    "resourcemanager.projects.list",
    "cloudasset.assets.searchAllResources",
]

# Billing-account-scoped: budget reads (all three skills)
BILLING_PERMS = [
    "billing.resourceAssociations.list",
    "billing.budgets.list",
]

# Minimal role that grants each permission
PERM_ROLE: Dict[str, str] = {
    "resourcemanager.projects.getIamPolicy":  "roles/viewer",
    "serviceusage.services.list":             "roles/viewer",
    "iam.serviceAccounts.list":               "roles/viewer",
    "iam.serviceAccountKeys.list":            "roles/iam.securityReviewer",
    "apikeys.keys.list":                      "roles/apikeys.viewer",
    "monitoring.timeSeries.list":             "roles/monitoring.viewer",
    "logging.logEntries.list":               "roles/logging.viewer",
    "run.services.list":                      "roles/viewer",
    "compute.instances.list":                 "roles/viewer",
    "sqladmin.instances.list":                "roles/viewer",
    "orgpolicy.policies.list":                "roles/viewer",
    "resourcemanager.projects.list":          "roles/browser",
    "cloudasset.assets.searchAllResources":   "roles/cloudasset.viewer",
    "billing.resourceAssociations.list":      "roles/billing.viewer",
    "billing.budgets.list":                   "roles/billing.viewer",
}

# Permission sets each skill depends on (for impact report)
SKILL_PERMS: Dict[str, Set[str]] = {
    "gcp-guardrails": set(PROJECT_PERMS + BILLING_PERMS),
    "gcp-api-cost-guard": {
        "apikeys.keys.list",
        "monitoring.timeSeries.list",
        "billing.resourceAssociations.list",
        "billing.budgets.list",
    },
    "gcp-project-cleanup": set(PROJECT_PERMS + ORG_PERMS + BILLING_PERMS),
}


def test_iam(resource_type: str, resource_id: str, perms: List[str]) -> Tuple[Set[str], Set[str]]:
    """Run testIamPermissions and return (have, missing).

    A failed testIamPermissions call is treated as all-missing so we never
    silently claim permissions the caller might not have.
    """
    cmd_map = {
        "project": ["projects", "test-iam-permissions", resource_id,
                    "--permissions", ",".join(perms)],
        "org":     ["organizations", "test-iam-permissions", resource_id,
                    "--permissions", ",".join(perms)],
        "billing": ["billing", "accounts", "test-iam-permissions", resource_id,
                    "--permissions", ",".join(perms)],
    }
    r = gcp.run(cmd_map[resource_type], timeout=30)
    if r["outcome"] != OK:
        return set(), set(perms)
    have = set(r["data"].get("permissions", []))
    return have, set(perms) - have


def grant_role(resource_type: str, resource_id: str,
               member: str, role: str, dry_run: bool) -> bool:
    """Add an IAM policy binding. Returns True on success (or dry-run)."""
    cmd_map = {
        "project": ["projects", "add-iam-policy-binding", resource_id],
        "org":     ["organizations", "add-iam-policy-binding", resource_id],
        "billing": ["billing", "accounts", "add-iam-policy-binding", resource_id],
    }
    cmd = cmd_map[resource_type] + [f"--member=user:{member}", f"--role={role}"]
    if dry_run:
        print(f"  [dry-run] gcloud {' '.join(cmd)}")
        return True
    r = gcp.run(cmd, timeout=60)
    if r["outcome"] != OK:
        print(f"  [!] Failed to grant {role}: {r['stderr'][:120]}", file=sys.stderr)
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Check and self-grant IAM roles for the gcp-* skills.")
    ap.add_argument("--project", required=True,
                    help="Any GCP project you own (used as the project-scope test target)")
    ap.add_argument("--fix", action="store_true",
                    help="Self-grant any missing roles")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print grant commands without running them")
    ap.add_argument("--json", action="store_true", dest="as_json",
                    help="Emit a JSON report to stdout instead of human output")
    args = ap.parse_args()

    account = gcp.current_account()
    if not account:
        print("No active gcloud account. Run: gcloud auth login", file=sys.stderr)
        return 1

    gaps: List[Dict[str, Any]] = []

    # ── Project scope ─────────────────────────────────────────────────────────
    _, miss = test_iam("project", args.project, PROJECT_PERMS)
    if miss:
        gaps.append({"scope": "project", "resource": args.project, "missing": sorted(miss)})

    # ── Org scope ─────────────────────────────────────────────────────────────
    r_orgs = gcp.run(["organizations", "list"], timeout=20)
    orgs = r_orgs["data"] if r_orgs["outcome"] == OK and r_orgs["data"] else []
    for org in orgs:
        org_id = str(org.get("name", "").split("/")[-1] or "")
        if not org_id:
            continue
        _, miss = test_iam("org", org_id, ORG_PERMS)
        if miss:
            gaps.append({"scope": "org", "resource": org_id, "missing": sorted(miss)})

    # ── Billing scope ─────────────────────────────────────────────────────────
    r_ba = gcp.run(["billing", "accounts", "list"], timeout=20)
    billing = r_ba["data"] if r_ba["outcome"] == OK and r_ba["data"] else []
    for ba in billing:
        ba_id = (ba.get("name") or "").split("/")[-1]
        if not ba_id:
            continue
        _, miss = test_iam("billing", ba_id, BILLING_PERMS)
        if miss:
            gaps.append({"scope": "billing", "resource": ba_id, "missing": sorted(miss)})

    # ── Report ────────────────────────────────────────────────────────────────
    report = {"account": account, "project": args.project, "gaps": gaps}
    if args.as_json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"\nAccount : {account}")
    print(f"Project : {args.project}\n")

    if not gaps:
        print("All required permissions are in place. The gcp-* skills are ready.")
        return 0

    total_missing = sum(len(g["missing"]) for g in gaps)
    print(f"Found {total_missing} missing permission(s) across {len(gaps)} scope(s):\n")
    for g in gaps:
        print(f"  [{g['scope']}: {g['resource']}]")
        for p in g["missing"]:
            role = PERM_ROLE.get(p, "unknown role")
            print(f"    {p:<50}  -> {role}")

    all_missing = {p for g in gaps for p in g["missing"]}
    print("\nSkill impact:")
    for skill, needed in SKILL_PERMS.items():
        blocked = all_missing & needed
        if blocked:
            print(f"  {skill:<26}  BLOCKED ({len(blocked)} permission(s) missing)")
        else:
            print(f"  {skill:<26}  OK")

    if not (args.fix or args.dry_run):
        print("\nRun with --dry-run to preview grant commands, or --fix to apply them.")
        return 1

    # ── Self-grant ────────────────────────────────────────────────────────────
    label = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{label}Granting missing roles...\n")
    errors = 0
    for g in gaps:
        roles_needed = sorted({PERM_ROLE[p] for p in g["missing"] if p in PERM_ROLE})
        for role in roles_needed:
            ok = grant_role(g["scope"], g["resource"], account, role, args.dry_run)
            if not ok:
                errors += 1

    if args.dry_run:
        print("\nRe-run with --fix to apply these grants.")
    elif errors:
        print(f"\nDone with {errors} error(s). You may not have admin rights on those scopes.")
        print("Copy the failed commands above and run them with an account that has admin rights.")
    else:
        print("\nDone. Re-run without --fix to verify all gaps are closed.")

    return 0 if (args.dry_run or not errors) else 1


if __name__ == "__main__":
    sys.exit(main())
