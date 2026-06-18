# Task 02 — Interview (intent on gaps only)

Goal: establish desired state = **baseline tier + per-person intent**, asking only what
config cannot tell you.

## Load or build the profile
Look for `profiles/<PROJ>.yaml` at the repo root.
- **Exists** → this is a re-audit. Load it. Treat profile as desired intent; the interview
  is now a *drift check* — only ask about members/services present live but NOT in the
  profile, or profile entries no longer present. Skip everything that still matches.
- **Missing** → first run. Build it from [../templates/profile.template.yaml](../templates/profile.template.yaml).

## Pick the baseline tier
Ask the user which tier fits this project (AskUserQuestion), one choice:
- `poc` — throwaway experiment; hard budget + kill-switch, GPU 0, Gemini/keys blocked.
- `app-dev` — an app's dev/staging project; capped budget + kill-switch.
- `app-prod` — an app's customer-facing project; alert-only budget, PAM + CI-deploy.
- `shared-ai` — the ONE key-quarantine project (not a team pool; Vertex is per-project).

Load that tier's block from [../config/tiers.yaml](../config/tiers.yaml) — the single
tunable source for budget, quotas, blocked APIs and role archetypes.

**Always confirm the money values.** Before anything lands in the apply-script, show the
user the tier's `budget.limit_usd` and `enforce_kill` and confirm or override them
(AskUserQuestion). These are defaults, never silent policy.

## Capture per-person intent (the differentiator)
From the scan's IAM `members`, list every **human/group** principal. For each (batch
obviously-similar ones), ask with AskUserQuestion what they do here, mapping the answer to
a role archetype in [../templates/roles/](../templates/roles/):
- "builds & deploys" → `developer`
- "reads data/dashboards" → `viewer`-class (e.g. `bigquery.dataViewer`)
- "uses Gemini/Vertex" → `ai-user`
- "admin" → keep elevated (note it as an accepted exception)
- "don't recognize / left" → mark for removal

Service-account principals: ask only if their role looks broader than a deploy SA needs.

Record every answer into the profile (`people:` with `goal` + `role`).

Proceed to [03-reconcile.md](03-reconcile.md).
