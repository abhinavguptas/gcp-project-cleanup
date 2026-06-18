---
name: gcp-api-cost-guard
description: Account-wide or single-project bird's-eye of every GCP API key and its cost exposure, then interactive guardrails (restrict keys, set budgets) to stop ad-hoc Gemini / AI Studio keys leaking spend. Handles "audit my api keys", "which keys are unrestricted", "bird's eye of api keys and cost", "find projects with no budget", "stop gemini key cost leaks", "restrict my api keys".
user-invocable: true
allowed-tools: Bash, Read, Write, AskUserQuestion
---

# gcp-api-cost-guard

## Purpose
Show every API key across an account (or one project) with its restriction state, Gemini
deadline status, and real call volume, alongside each project's billing + budget state; then
walk the findings and emit a reviewed gcloud script that restricts keys and sets budgets. The
target failure is an ad-hoc AI Studio Gemini key billing against a project with no budget.

Let `SKILL` = this skill's directory (`.claude/skills/gcp-api-cost-guard`). Run the engine as
`python3 "$SKILL/scripts/scan_keys.py"` from the repo root. The engine imports the shared
`gcp-py` library (a sibling skill) for GCP transport and key/cost primitives.

## Preconditions
- `gcloud` installed and authenticated (`gcloud auth list` shows an account); if none, tell
  the user to run `! gcloud auth login` and stop.
- Working directory is the repo root, since the report resolves to repo-root `reports/`.

## Inputs
- `account` (optional) — credentialed account to target; default is the active gcloud account.
- `quota_project` (required only when `account` is NOT the active account) — any project that
  account owns, for API quota.
- `scope` (optional) — `account-wide` (default) or a single `project` id.
- `intent` (optional) — scan / review / apply; if absent, inferred from state below.

## Outputs
- `reports/keys_report.<account-slug>.json` — the scan report (gitignored).
- `reports/apply-keys.<account-slug>.sh` — the reviewed apply-script (written in step-03).

## Routing
Pick the phase by the user's words, then by state. Read only the matching step file.

| Intent (words) | State | Step |
|---|---|---|
| scan, audit, "bird's eye", "which keys", inventory | (any) | [references/step-01-scan.md](references/step-01-scan.md) |
| review, triage, "what should I fix", decide | report exists | [references/step-02-review.md](references/step-02-review.md) |
| apply, fix, restrict, "set budget", harden | choices made | [references/step-03-apply.md](references/step-03-apply.md) |

State fallback: no report -> scan; report exists, no apply-script -> review; otherwise apply.

## Hard rules
- Read-only until the apply phase, because the scan and review must never mutate a project the
  user has not approved changes to.
- The emitted apply-script is additive: key restrictions and budget creation run as-is;
  destructive lines (key delete, breaking migration) are commented `# REVIEW:`, to prevent an
  unattended run from breaking a live integration.
- A failed read is a `data_gap` finding, never a clean bill, since "could not read keys" must
  not be shown as "no keys".
- Never claim per-key dollar figures, because GCP attributes cost to project/service/SKU only;
  per-key cost is call volume + services.
