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
Billing + budget primitives.

GCP exposes no per-project cost API, so this reports the billing-enabled flag and which
budgets cover a project - not dollars. Pure functions; callers decide what the absence of a
budget means. Import via the gcp-py shim (see SKILL.md); requires sibling `gcp` on sys.path.
"""

from typing import Any, Dict, List

import gcp

# Per-billing-account project that successfully listed budgets. The billing-budgets API
# routes quota through a project; the global core/project may be deleted or unusable by the
# scanning account. Once any project linked to a billing account lists its budgets, reuse it
# for the account's other projects so a shared billing account is read once, not retried each.
_budget_proxy: Dict[str, str] = {}


def project_billing(project: str) -> Dict[str, Any]:
    """Billing-enabled flag + billing account: {status, billing_enabled, billing_account}."""
    r = gcp.run(["billing", "projects", "describe", project], timeout=20)
    if r["outcome"] != gcp.OK:
        return {"status": r["outcome"], "billing_enabled": None, "billing_account": None}
    d = r["data"] or {}
    return {"status": gcp.OK,
            "billing_enabled": bool(d.get("billingEnabled", False)),
            "billing_account": d.get("billingAccountName") or None}


def project_budgets(project: str) -> Dict[str, Any]:
    """Budgets that cover a project.

    Budgets live on the billing account, not the project. A budget filter stores projects as
    projects/<id> OR projects/<number>, so both are resolved - otherwise a number-keyed budget
    is missed and the project is wrongly flagged as unbudgeted. A budget with no project filter
    is account-wide (covers this project but may also filter by service).

    Returns {status, billing_enabled, billing_account, budgets, has_budget, has_project_budget}.
    """
    b = project_billing(project)
    empty = {"billing_enabled": b["billing_enabled"], "billing_account": b["billing_account"],
             "budgets": [], "has_budget": False, "has_project_budget": False}
    if b["status"] != gcp.OK:
        return {"status": b["status"], **empty}
    account = b["billing_account"]
    if not account:
        return {"status": gcp.OK, **empty}
    account_id = account.split("/")[-1]
    base = ["billing", "budgets", "list", "--billing-account", account_id]
    # If another project on this billing account already proved a working route, use it.
    proxy = _budget_proxy.get(account_id)
    used = proxy
    lr = gcp.run(base + (["--billing-project", proxy] if proxy else []), timeout=30)
    if lr["outcome"] != gcp.OK and proxy != project:
        # The default route (global core/project) failed: it may be deleted or the account
        # may lack serviceusage there. Retry routed through the scanned project, which is
        # linked to this billing account - otherwise a real budget is read as a data gap.
        used = project
        lr = gcp.run(base + ["--billing-project", project], timeout=30)
    if lr["outcome"] != gcp.OK:
        return {"status": lr["outcome"], **empty, "billing_account": account_id}
    if used:
        _budget_proxy[account_id] = used
    refs = {f"projects/{project}"}
    pn = gcp.run(["projects", "describe", project, "--format=value(projectNumber)"], parse_json=False)
    if pn["outcome"] == gcp.OK and (pn["data"] or "").strip():
        refs.add(f"projects/{pn['data'].strip()}")
    matched: List[Dict[str, Any]] = []
    for bud in (lr["data"] or []):
        if not isinstance(bud, dict):
            continue
        projs = (bud.get("budgetFilter", {}) or {}).get("projects") or []
        if set(projs) & refs:
            scope = "project"
        elif not projs:
            scope = "account-wide"
        else:
            continue  # scoped to OTHER projects only
        matched.append({
            "name": bud.get("displayName", "?"),
            "scope": scope,
            "amount": bud.get("amount", {}),
            # thresholdPercent is a FRACTION (0.9 = 90%); a 100% rule is often omitted entirely.
            "thresholds_pct": [round((t.get("thresholdPercent") or 0) * 100)
                               for t in bud.get("thresholdRules", [])] or [100],
        })
    return {"status": gcp.OK, "billing_enabled": b["billing_enabled"], "billing_account": account_id,
            "budgets": matched, "has_budget": bool(matched),
            "has_project_budget": any(m["scope"] == "project" for m in matched)}
