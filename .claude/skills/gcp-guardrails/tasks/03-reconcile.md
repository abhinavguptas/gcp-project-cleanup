# Task 03 — Reconcile (right-size to intent)

Goal: turn every finding into a concrete fix or an explicitly accepted exception. Nothing
is left ambiguous; nothing is applied yet.

## Resolve each finding
Walk the report's `findings` (high→info). For each, the resolution comes from the baseline
tier + the intent captured in 02:

| Finding kind | Default resolution |
|---|---|
| `over_privilege` | swap to the role archetype matching the person's stated goal (see [../templates/roles/](../templates/roles/)); if "admin", record an accepted exception. A `(conditional/time-boxed)` grant is usually fine (PAM) — leave it. |
| `stale_member` | remove the binding — it's a DELETED principal still holding access. Always safe to propose. |
| `risk_api_enabled` | if the tier blocks it (e.g. `generativelanguage` outside `shared-ai`) → disable + note; else keep |
| `user_managed_sa_key` | flag for deletion + recommend Workload Identity / short-lived creds |
| `unrestricted_key` | add an API restriction (the key can hit any enabled API incl. Gemini) per [../data/risk-reference.md](../data/risk-reference.md) |
| `underrestricted_key` | add an app (referrer/IP) restriction; lower urgency than `unrestricted_key` |
| `no_budget` | create a budget from the tier amount; `enforce_kill` per tier |
| `only_account_wide_budget` | add a project-specific budget so this project can't hide spend under the account total |
| `unbounded_service` | set Cloud Run maxScale to the tier cap; minScale 0 unless intent says always-warm; remove GPU unless justified |
| `gpu_vm` / `large_vm_running` | confirm with the user the VM should be on; if not, propose stop/delete (commented `# REVIEW:`) |
| `running_vms` / `cloud_sql_instances` | info — surface the cost; act only if intent says they're unneeded |
| `not_verified` | info only — note that org-policy inheritance must be checked at org/folder level, never fix from here |
| `data_gap` | tell the user which section couldn't be read and why; do not assume safe |

## Confirm exceptions, don't bulldoze
For anything the intent justifies keeping (an admin, an always-warm prod service, a needed
key), record it in the profile under `exceptions:` with the reason. Re-audits then treat it
as expected, not a finding.

## Run the reviewer checklist
Before emitting, walk [../checklists/posture-checklist.md](../checklists/posture-checklist.md)
to confirm nothing was missed and no fix is destructive-by-surprise.

Proceed to [04-emit-script.md](04-emit-script.md).
