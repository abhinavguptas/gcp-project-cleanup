# Risk reference — why each finding matters and how to fix it

Knowledge base for the interview. Keep terse; this is reasoning fuel, not docs.

Keys, budgets, service-account-key, and cost-attribution facts live in the shared
[../../gcp-py/data/risk-reference.md](../../gcp-py/data/risk-reference.md). This file holds
only the topics specific to this skill's broader posture audit.

## Over-privilege
- `roles/owner` / `roles/editor` on a human is the usual root cause of accidental changes
  and spend. Replace with the narrowest archetype in ../templates/roles/.
- Elevation when genuinely needed → **Privileged Access Manager (PAM)**: time-bound,
  approval-gated grants, auto-revoked. Beats standing access.

## Runaway bills (Cloud Run + friends)
- Four cost knobs: max-instances autoscaling, min-instances > 0 (always-warm), GPU,
  oversized CPU/memory. Cap them, don't trust memory.
- Enforce max-instances org-wide via a **Cloud Run custom org-policy constraint** (CEL on
  maxInstanceCount, enforced on CREATE+UPDATE) — admin-level, outside this skill.
