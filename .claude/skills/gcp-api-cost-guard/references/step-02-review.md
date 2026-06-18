# Step 02 — Review

Goal: turn each finding into a chosen action, recorded for the apply phase. Read-only.

1. Locate `reports/keys_report.<account-slug>.json`; if it is absent, run the scan first.
2. Walk projects in report order (already sorted worst-first). For each project, present its
   findings and ask the user what to do. Use one AskUserQuestion per finding, or batch a
   project's findings into a single multi-select when they share an action. Options by `kind`:
   - `unrestricted_key` -> Restrict to Gemini | Migrate to auth key (note) | Delete | Skip
   - `standard_key` -> Plan auth-key migration (note) | Skip
   - `no_budget` -> Set budget (ask the USD amount) | Skip
   - `only_account_budget` -> Add a project budget (ask the USD amount) | Skip
   - `idle_key` -> Delete | Skip
   - `data_gap` -> cannot action; surface and Skip.
3. Record each choice in memory for the apply phase: project_id, billing_account, kind, key
   uid/name, action, and any budget amount or app-restriction (referrers/IPs) the user gave.
4. When every finding is decided, offer to emit the apply-script.
