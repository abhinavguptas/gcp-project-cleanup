---
slug: gcp-iam-setup
ts: 2026-06-18T17:00:13+05:30
kind: note
called_by: cio-deep-worker
evidence:
  - "gcp-guardrails/templates/create-project.template.sh L19-22 (proven peer pattern)"
  - "gcloud billing budgets create reference: basis values lowercase, filter-projects=projects/{project_id}"
---

Applied fixes (medium severity) to gcp-iam-setup budget commands in apply.md and plan.md:
1. --filter-projects changed from projects/<project_number> to projects/<project_id>. The flag expects project ID, and the peer skills gcp-guardrails + gcp-api-cost-guard already use project ID. Removed the now-unneeded "get project number" step in apply.md.
2. --threshold-rule changed from percent=X,basis=CURRENT_SPEND to percent=X. basis must be lowercase (current-spend); default is current-spend so the basis token was dropped to match the proven peer pattern.
3. --budget-amount unquoted ("100USD" -> 100USD); both work but matches peers.

Completeness fixes applied:
4. Added alloydb optional-bundle role grants (roles/alloydb.admin to PM, roles/alloydb.client to dev) to apply.md; baseline defines the bundle but apply.md omitted it.
5. Added prod-only constraints/run.allowedIngress org policy block (is:internal-and-cloud-load-balancing) to apply.md and plan.md; baseline prod_delta defines it but both files omitted it.

Verified correct, no change needed: --stage=GA (valid), gcp.resourceLocations in:<region>-locations format, enable-enforce + set-policy commands exist in gcloud 565.0.0 and are not deprecated, set-policy --project flag valid.
