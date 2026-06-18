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
Posture audit for ONE GCP project we intend to KEEP.

This is the prevention/guardrail counterpart to scan_projects.py (which decides what
to DELETE). It is strictly read-only: it maps a project's posture - enabled services,
IAM members->roles, service-account keys, API keys, budget, inherited org policies,
and expensive live resources (Cloud Run, running Compute VMs, Cloud SQL) - and flags
the gaps that cause accidental bills or over-privilege.

It never mutates anything and never proposes deletion. Its output (structured JSON +
a human summary) is the evidence the gcp-guardrails interview reasons over: the model asks
about intent only where config is ambiguous, then emits a reviewed gcloud apply-script.

Same guarantee as the shared gcp-py helpers: a FAILED call is never reported as "found
nothing". Every
section carries its own `outcome`, so the interview can tell "no over-privilege" apart
from "couldn't read IAM".
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

# Import the shared gcp-py library (sibling skill under .claude/skills/) - see gcp-py/SKILL.md.
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "gcp-py", "scripts"))
import gcp
from gcp import OK, DISABLED
import keys
import cost

# APIs whose mere enablement is a cost/security signal worth surfacing. generativelanguage
# + apikeys are the Gemini exposure (enabling Gemini silently grants existing keys access);
# the rest are the money surface that drives runaway bills.
RISK_APIS = {
    "generativelanguage.googleapis.com": ("high", "Consumer Gemini API - prefer Vertex AI; quarantine to one sandbox project"),
    "apikeys.googleapis.com": ("high", "API Keys API - enables ungoverned key creation"),
    "run.googleapis.com": ("info", "Cloud Run - check max-instances / GPU below"),
    "compute.googleapis.com": ("info", "Compute Engine - check VM sizes / quotas"),
    "container.googleapis.com": ("info", "GKE - high cost surface"),
    "aiplatform.googleapis.com": ("info", "Vertex AI - the governed path for Gemini (good)"),
    "sqladmin.googleapis.com": ("info", "Cloud SQL - always-on cost"),
}

# Basic (legacy) roles. Held by a human or group, these are the classic over-privilege
# finding: almost nobody needs project-wide owner/editor.
BASIC_ROLES = {
    "roles/owner": "high",
    "roles/editor": "high",
    "roles/viewer": "low",
}

# Machine types cheap enough to ignore when left running; anything larger (or any GPU) is a
# likely forgotten-cost worth surfacing - the classic "spun it up, forgot to stop it" bill.
SMALL_MACHINES = {"e2-micro", "e2-small", "e2-medium", "f1-micro", "g1-small"}

def section(outcome: str, data: Any, note: str = "") -> Dict[str, Any]:
    return {"outcome": outcome, "data": data, "note": note}


def scan_services(project: str) -> Dict[str, Any]:
    r = gcp.run(["services", "list", "--enabled", "--project", project])
    if r["outcome"] != OK:
        return section(r["outcome"], None, r["stderr"])
    enabled = sorted(s.get("config", {}).get("name") or s.get("name", "") for s in r["data"])
    flagged = [
        {"api": a, "severity": RISK_APIS[a][0], "why": RISK_APIS[a][1]}
        for a in enabled if a in RISK_APIS
    ]
    return section(OK, {"enabled_count": len(enabled), "flagged": flagged, "all": enabled})


def scan_iam(project: str) -> Dict[str, Any]:
    r = gcp.run(["projects", "get-iam-policy", project])
    if r["outcome"] != OK:
        return section(r["outcome"], None, r["stderr"])
    members: Dict[str, Dict[str, Any]] = {}
    cond: Dict[str, set] = {}    # member -> roles granted via a CONDITIONAL binding
    uncond: Dict[str, set] = {}  # member -> roles granted via an UNCONDITIONAL binding
    for b in r["data"].get("bindings", []):
        role = b.get("role", "")
        conditional = bool(b.get("condition"))
        for m in b.get("members", []):
            # Deleted principals arrive as `deleted:user:email?uid=...`; strip the prefix so the
            # real kind (user/serviceAccount) is recovered, and drop the ?uid suffix.
            deleted = m.startswith("deleted:")
            body = m[len("deleted:"):] if deleted else m
            kind, _, ident = body.partition(":")
            entry = members.setdefault(m, {"kind": kind, "id": ident.split("?", 1)[0],
                                           "deleted": deleted, "roles": []})
            if role not in entry["roles"]:  # dedupe: same role can appear in >1 binding
                entry["roles"].append(role)
            (cond if conditional else uncond).setdefault(m, set()).add(role)
    # Over-privilege findings: basic roles, weighted higher for humans/groups than for SAs
    # (a deploy SA legitimately needs broad rights; a person rarely does). Deleted principals
    # are reported separately as stale_members, not here, to avoid double-counting.
    overprivileged = []
    for m, e in members.items():
        if e.get("deleted"):
            continue
        for role in e["roles"]:
            if role in BASIC_ROLES:
                sev = BASIC_ROLES[role]
                if e["kind"] in ("user", "group") and role != "roles/viewer":
                    sev = "high"
                elif e["kind"] == "serviceAccount":
                    sev = "medium"
                # A role granted ONLY via a condition (e.g. PAM time-boxed elevation, which we
                # recommend) is not standing over-privilege - downgrade so we don't flag it hard.
                time_boxed = role in cond.get(m, set()) and role not in uncond.get(m, set())
                if time_boxed:
                    sev = "low"
                overprivileged.append({"member": m, "kind": e["kind"], "role": role,
                                       "severity": sev, "conditional": time_boxed})
    stale_members = [{"member": m, "roles": e["roles"]} for m, e in members.items() if e.get("deleted")]
    return section(OK, {"members": members, "overprivileged": overprivileged,
                        "stale_members": stale_members})


def scan_sa_keys(project: str) -> Dict[str, Any]:
    r = keys.sa_user_keys(project)
    if r["status"] != OK:
        return section(r["status"], None, "service accounts unreadable")
    sas = r["service_accounts"]
    # user_key_count is None when that SA's key list could not be read; treat only confirmed
    # downloadable keys as a finding (matches the prior skip-on-failure behaviour).
    user_keyed = [{"sa": s["email"], "user_keys": s["user_key_count"]}
                  for s in sas if s["user_key_count"]]
    return section(OK, {"sa_count": len(sas), "user_managed_keys": user_keyed})


def scan_api_keys(project: str) -> Dict[str, Any]:
    r = keys.list_keys(project)
    if r["status"] == DISABLED:  # API off => no keys can exist => confirmed safe
        return section(OK, {"key_count": 0, "unrestricted": []}, "apikeys API disabled")
    if r["status"] != OK:
        return section(r["status"], None, r["note"])
    klist = r["keys"]
    unrestricted = []
    for k in klist:
        c = keys.classify_key(k)
        if not c["api_restricted"] or not c["app_restricted"]:
            unrestricted.append({
                "key": k.get("displayName") or k.get("uid", "?"),
                "api_restricted": c["api_restricted"],
                "app_restricted": c["app_restricted"],
            })
    return section(OK, {"key_count": len(klist), "unrestricted": unrestricted})


def scan_budget(project: str) -> Dict[str, Any]:
    b = cost.project_budgets(project)
    if b["status"] != OK:
        return section(b["status"], None, "billing/budget unreadable")
    return section(OK, {"billing_enabled": b["billing_enabled"],
                        "billing_account": b["billing_account"],
                        "budgets_for_project": b["budgets"],
                        "has_budget": b["has_budget"],
                        "has_project_budget": b["has_project_budget"]})


def scan_org_policies(project: str) -> Dict[str, Any]:
    # Project scope shows only DIRECTLY-set policies; inherited/effective guardrails need an
    # org-level check (`describe --effective`). So report what's set here and flag honestly
    # when we can't verify, rather than false-flagging inherited policies as "missing".
    r = gcp.run(["org-policies", "list", "--project", project])
    if r["outcome"] != OK:
        return section(OK, {"verified": False, "reason": r["outcome"], "set_here": []})
    set_here = sorted({f"constraints/{p.get('name', '').split('/')[-1]}" for p in (r["data"] or [])})
    return section(OK, {"verified": True, "set_here": set_here})


def scan_cloud_run(project: str) -> Dict[str, Any]:
    # No --region: managed Cloud Run lists across all regions via the global endpoint.
    # (--region "-" builds an invalid regional endpoint and errors.)
    r = gcp.run(["run", "services", "list", "--project", project])
    if r["outcome"] == DISABLED:  # Cloud Run off => no services => confirmed safe
        return section(OK, {"service_count": 0, "risky": []}, "run API disabled")
    if r["outcome"] != OK:
        return section(r["outcome"], None, r["stderr"])
    risky = []
    for svc in (r["data"] or []):
        name = svc.get("metadata", {}).get("name") or svc.get("name", "?")
        max_i, min_i = _run_scale(svc)
        # Unbounded autoscaling (no/large maxScale), always-warm (minScale>0), and GPU are the
        # Cloud Run cost knobs. Flag all three. Scale fields come from either the v1 Knative
        # annotations or the v2 template.scaling shape (see _run_scale).
        issues = []
        if max_i is None or (str(max_i).isdigit() and int(max_i) > 100):
            issues.append(f"maxScale={max_i if max_i is not None else 'unset'} (unbounded autoscaling)")
        if min_i is not None and str(min_i).isdigit() and int(min_i) > 0:
            issues.append(f"minScale={min_i} (always-warm billing)")
        if "nvidia.com/gpu" in json.dumps(svc):
            issues.append("GPU attached (high hourly cost)")
        if issues:
            risky.append({"service": name, "issues": issues})
    return section(OK, {"service_count": len(r["data"] or []), "risky": risky})


def _run_scale(svc: Dict[str, Any]):
    """Return (maxScale, minScale) as strings/None, reading either the v1 Knative annotation
    shape or the v2 template.scaling shape - gcloud emits one or the other by version."""
    ann = svc.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {})
    max_i = ann.get("autoscaling.knative.dev/maxScale")
    min_i = ann.get("autoscaling.knative.dev/minScale")
    if max_i is None and min_i is None:  # fall back to v2 shape
        scaling = svc.get("template", {}).get("scaling", {})
        if scaling.get("maxInstanceCount") is not None:
            max_i = str(scaling["maxInstanceCount"])
        if scaling.get("minInstanceCount") is not None:
            min_i = str(scaling["minInstanceCount"])
    return max_i, min_i


def scan_compute(project: str) -> Dict[str, Any]:
    r = gcp.run(["compute", "instances", "list", "--project", project])
    if r["outcome"] == DISABLED:  # Compute off => no VMs => confirmed safe
        return section(OK, {"running": [], "total": 0}, "compute API disabled")
    if r["outcome"] != OK:
        return section(r["outcome"], None, r["stderr"])
    running = []
    for inst in (r["data"] or []):
        if inst.get("status") != "RUNNING":
            continue
        running.append({"name": inst.get("name", "?"),
                        "machine_type": (inst.get("machineType") or "").split("/")[-1],
                        "gpu": bool(inst.get("guestAccelerators"))})
    return section(OK, {"running": running, "total": len(r["data"] or [])})


def scan_sql(project: str) -> Dict[str, Any]:
    r = gcp.run(["sql", "instances", "list", "--project", project])
    if r["outcome"] == DISABLED:  # Cloud SQL off => no instances => confirmed safe
        return section(OK, {"instances": []}, "sqladmin API disabled")
    if r["outcome"] != OK:
        return section(r["outcome"], None, r["stderr"])
    inst = [{"name": i.get("name", "?"), "tier": (i.get("settings") or {}).get("tier", "?")}
            for i in (r["data"] or [])]
    return section(OK, {"instances": inst})


def build_findings(sections: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten the per-section signals into a single severity-ranked findings list -
    the worklist the interview walks. Sections that could not be read become an info
    'data_gap' finding so nothing silently passes."""
    out: List[Dict[str, Any]] = []

    def gap(name: str, sec: Dict[str, Any]):
        if sec["outcome"] != OK:
            # First line only, capped - never dump the full multi-line gcloud stderr at the user.
            note = (sec.get("note", "") or "").splitlines()[0][:140] if sec.get("note") else ""
            out.append({"severity": "info", "area": name, "kind": "data_gap",
                        "detail": f"could not read ({sec['outcome']}): {note}"})
            return True
        return False

    svc = sections["services"]
    if not gap("services", svc):
        for f in svc["data"]["flagged"]:
            if f["severity"] != "info":
                out.append({"severity": f["severity"], "area": "services",
                            "kind": "risk_api_enabled", "detail": f["api"] + " - " + f["why"]})

    iam = sections["iam"]
    if not gap("iam", iam):
        for s in iam["data"].get("stale_members", []):
            out.append({"severity": "high", "area": "iam", "kind": "stale_member",
                        "detail": f"deleted principal {s['member']} still holds "
                                  f"{', '.join(s['roles'])} - remove the binding"})
        for o in iam["data"]["overprivileged"]:
            note = " (conditional/time-boxed)" if o.get("conditional") else ""
            out.append({"severity": o["severity"], "area": "iam", "kind": "over_privilege",
                        "detail": f"{o['member']} holds {o['role']}{note}"})

    sak = sections["sa_keys"]
    if not gap("sa_keys", sak):
        for k in sak["data"]["user_managed_keys"]:
            out.append({"severity": "high", "area": "sa_keys", "kind": "user_managed_sa_key",
                        "detail": f"{k['sa']} has {k['user_keys']} downloadable key(s)"})

    ak = sections["api_keys"]
    if not gap("api_keys", ak):
        for k in ak["data"]["unrestricted"]:
            if not k["api_restricted"]:
                out.append({"severity": "high", "area": "api_keys", "kind": "unrestricted_key",
                            "detail": f"key '{k['key']}' has NO API restriction - can call any "
                                      f"enabled API including Gemini; restrict it"})
            else:
                out.append({"severity": "medium", "area": "api_keys", "kind": "underrestricted_key",
                            "detail": f"key '{k['key']}' is API-restricted but has no app "
                                      f"(referrer/IP) restriction; add one"})

    bud = sections["budget"]
    if not gap("budget", bud):
        if bud["data"].get("billing_enabled"):
            if not bud["data"]["has_budget"]:
                out.append({"severity": "high", "area": "budget", "kind": "no_budget",
                            "detail": "billing enabled but NO budget covers this project"})
            elif not bud["data"].get("has_project_budget"):
                out.append({"severity": "medium", "area": "budget", "kind": "only_account_wide_budget",
                            "detail": "only an account-wide budget covers this project; "
                                      "add a project-specific budget so this project can't hide spend"})

    run_ = sections["cloud_run"]
    if not gap("cloud_run", run_):
        for s in run_["data"]["risky"]:
            out.append({"severity": "medium", "area": "cloud_run", "kind": "unbounded_service",
                        "detail": f"{s['service']}: " + "; ".join(s["issues"])})

    comp = sections["compute"]
    if not gap("compute", comp):
        for vm in comp["data"]["running"]:
            if vm["gpu"]:
                out.append({"severity": "high", "area": "compute", "kind": "gpu_vm",
                            "detail": f"VM {vm['name']} is RUNNING with a GPU ({vm['machine_type']}) - high hourly cost"})
            elif vm["machine_type"] not in SMALL_MACHINES:
                out.append({"severity": "medium", "area": "compute", "kind": "large_vm_running",
                            "detail": f"VM {vm['name']} ({vm['machine_type']}) is running - confirm it should be on"})
        small = [v for v in comp["data"]["running"] if not v["gpu"] and v["machine_type"] in SMALL_MACHINES]
        if small:
            out.append({"severity": "info", "area": "compute", "kind": "running_vms",
                        "detail": f"{len(small)} small VM(s) running: " + ", ".join(v["name"] for v in small)})

    sql = sections["sql"]
    if not gap("sql", sql):
        if sql["data"]["instances"]:
            out.append({"severity": "info", "area": "sql", "kind": "cloud_sql_instances",
                        "detail": f"{len(sql['data']['instances'])} Cloud SQL instance(s) (always-on cost): "
                                  + ", ".join(i["name"] for i in sql["data"]["instances"])})

    org = sections["org_policies"]
    if not gap("org_policies", org):
        if not org["data"].get("verified"):
            out.append({"severity": "info", "area": "org_policies", "kind": "not_verified",
                        "detail": f"org-policy inheritance not checked at project scope "
                                  f"({org['data']['reason']}); verify guardrails at org/folder level"})

    order = {"high": 0, "medium": 1, "low": 2, "info": 3}
    out.sort(key=lambda f: order.get(f["severity"], 9))
    return out


def audit(project: str) -> Dict[str, Any]:
    sections = {
        "services": scan_services(project),
        "iam": scan_iam(project),
        "sa_keys": scan_sa_keys(project),
        "api_keys": scan_api_keys(project),
        "budget": scan_budget(project),
        "org_policies": scan_org_policies(project),
        "cloud_run": scan_cloud_run(project),
        "compute": scan_compute(project),
        "sql": scan_sql(project),
    }
    return {"project": project, "account": gcp.current_account(),
            "sections": sections, "findings": build_findings(sections)}


def print_summary(report: Dict[str, Any]) -> None:
    f = report["findings"]
    counts = {s: sum(1 for x in f if x["severity"] == s) for s in ("high", "medium", "low", "info")}
    print(f"\nPosture audit: {report['project']}  (account: {report['account']})")
    print(f"  findings: {counts['high']} high · {counts['medium']} medium · "
          f"{counts['low']} low · {counts['info']} info\n")
    for x in f:
        tag = {"high": "!!", "medium": "! ", "low": "  ", "info": "i "}.get(x["severity"], "  ")
        print(f"  [{tag}] {x['area']:<12} {x['kind']:<22} {x['detail']}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only posture audit of one GCP project.")
    ap.add_argument("--project", required=True, help="project ID to audit")
    ap.add_argument("--account", help="credentialed account to use (no config change)")
    ap.add_argument("--quota-project", help="a project this account owns (for cross-org quota)")
    ap.add_argument("--out", help="write JSON report here (default: <skill>/output/audit_report.<project>.json)")
    ap.add_argument("--json", action="store_true", help="print JSON to stdout instead of summary")
    args = ap.parse_args()

    if args.account:
        gcp.set_account(args.account, args.quota_project)

    report = audit(args.project)
    # Keep generated artifacts inside the skill home (../output), not the caller's cwd, so the
    # skill stays self-contained. Path(__file__) -> scripts/, parent -> skill root.
    if args.out:
        out_path = args.out
    else:
        out_dir = Path(__file__).resolve().parent.parent / "output"
        out_dir.mkdir(exist_ok=True)
        out_path = str(out_dir / f"audit_report.{args.project}.json")
    with open(out_path, "w") as fh:
        json.dump(report, fh, indent=2)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_summary(report)
        print(f"  full report: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
