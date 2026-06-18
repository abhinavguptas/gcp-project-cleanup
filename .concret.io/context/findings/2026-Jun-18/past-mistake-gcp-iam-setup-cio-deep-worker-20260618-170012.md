---
slug: gcp-iam-setup
ts: 2026-06-18T17:00:12+05:30
kind: past-mistake
called_by: cio-deep-worker
evidence:
  - "apply.md L170-183 allowedValues:[\"{{ org_domain }}\"]"
  - "GCP docs resource-manager/docs/organization-policy/restricting-domains require directoryCustomerId"
---

HIGH severity, NOT YET APPLIED (needs user decision).

constraints/iam.allowedPolicyMemberDomains requires the Google Workspace Customer ID (format is:C01abc23) in allowedValues, NOT a raw domain string. apply.md Section 4 writes allowedValues: ["{{ org_domain }}"] which resolves to e.g. "concret.io". gcloud set-policy rejects this (or applies a policy that matches nothing, locking out all member additions).

Fix: resolve the customer ID at apply time via
  gcloud organizations list --format="value(owner.directoryCustomerId)"
then write allowedValues: ["is:<customerId>"]. seed.md should collect/derive customer ID, and the baseline YAML org_domain comment is misleading for this constraint (org_domain is still correct for human-readable display but not for this org-policy value).
