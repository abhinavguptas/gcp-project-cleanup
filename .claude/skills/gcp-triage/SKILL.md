---
name: gcp-triage
description: Deep-dive the cleanup candidates from a scan report and build a confirmed delete worklist. Shows per-project evidence (signals, API keys, last admin activity, resources) and asks keep/delete/recycle for each. Use after /gcp-scan when the user wants to decide what to remove ("review candidates", "triage projects", "what should I delete").
user-invocable: true
allowed-tools: Bash, Read, AskUserQuestion, Write
---

# gcp-triage

Walk the user through the cleanup candidates one at a time and produce a confirmed
worklist of project IDs to delete. Reads a report from `/gcp-scan`; writes a worklist
that `/gcp-purge` consumes. Repo root: `/Users/abhinav/code/gcloud`.

## 1. Locate the report
List `projects_report.*.json`. If more than one, ask which account to triage. Load it.
Candidates = projects where `decision.recommendation` is `review`, `recycle_keys`, or
`delete`. If none, say so and stop.

## 2. Triage each candidate
Order by deletability: `delete` first, then `recycle_keys`, then `review` (oldest
`activity.last_admin_action` first). For each, show a compact evidence block:
- recommendation + confidence + reasons + blockers + data_gaps
- `signals` (traffic / live keys / recent admin / metadata age)
- resources total + `coverage_complete`; API keys with per-key `calls_in_window`/`risk`
- last admin action (date + principal); billing enabled?

Then ask with AskUserQuestion: **delete / recycle keys / keep / skip / deep-dive**.
- "deep-dive" → print that project's full JSON record and re-ask.
- Batch obviously-similar rows into one question when sensible (e.g. several empty
  `sys-*` projects), but never bundle a project that has any blocker or data_gap.

Never pre-select delete for a project with a blocker (live traffic / recent admin / live
key) or with `coverage_complete: false` — those need an explicit human choice.

## 3. Write the worklist
Collect the project IDs marked **delete** into `worklist.<account-slug>.txt` (one ID per
line) via Write, and echo the final list back to the user. Keep `recycle_keys` choices in
a separate note for the future key-security pass.

## 4. Hand off
If the worklist is non-empty, tell the user to run `/gcp-purge` (or offer to invoke it),
passing the same account. If empty, confirm nothing was selected.
