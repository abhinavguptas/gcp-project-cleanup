# Posture reviewer checklist

Run before emitting the apply-script (task 03). Each item: pass, or record an exception.

## Coverage
- [ ] Every scan section returned `ok` — any `data_gap` surfaced to the user, not assumed safe.
- [ ] Billing checked: project has a budget (or one explicitly created in the script).

## Least privilege
- [ ] No human/group holds `roles/owner` or `roles/editor` without a recorded exception.
- [ ] Every person maps to a role archetype matching their stated goal.
- [ ] Unrecognized / departed members marked for removal (commented `# REVIEW:`).

## Keys & secrets
- [ ] No user-managed service-account keys (or flagged for deletion + WIF recommended).
- [ ] Every API key has both API and application restrictions.
- [ ] `generativelanguage` / `apikeys` APIs enabled ONLY if tier is `shared-ai`.

## Cost shape
- [ ] Cloud Run services within the tier's max-instances cap; minScale 0 unless intent says warm.
- [ ] GPU quota 0 unless a workload justifies it.

## Safety of the script itself
- [ ] Additive/tightening only; every removal is commented `# REVIEW:`.
- [ ] No org-level policy mutation (project scope only).
- [ ] Re-audit hint included so convergence is verifiable.
