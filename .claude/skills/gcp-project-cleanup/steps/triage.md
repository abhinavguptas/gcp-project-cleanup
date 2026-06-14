# Step: triage

Walk the user through the scan candidates and produce a confirmed delete worklist. `$SKILL`
is the skill folder.

## 1. Locate the report
List `reports/projects_report.*.json` (repo root). If more than one, ask which account. Load it.
Candidates = projects where `decision.recommendation` is `review`, `recycle_keys`, or
`delete`. If none, say so and stop.

## 2. Triage each candidate
Order by deletability: `delete`, then `recycle_keys`, then `review` (oldest
`activity.last_admin_action` first). For each, show a compact evidence block:
- recommendation + confidence + reasons + blockers + data_gaps
- `signals` (traffic / live keys / recent admin / metadata age)
- resources total + `coverage_complete`; API keys with per-key `calls_in_window`/`risk`
- last admin action (date + principal); billing enabled?

Then ask with AskUserQuestion: **delete / recycle keys / keep / skip / deep-dive**.
- "deep-dive" → print that project's full JSON record (and live `gcloud asset
  search-all-resources` / `services list` if the user asks what's deployed), then re-ask.
- Batch obviously-similar rows into one question when sensible (e.g. several empty `sys-*`
  projects), but never bundle a project with any blocker or data_gap, because those need an
  explicit human choice.

Never pre-select delete for a project with a blocker (live traffic / recent admin / live
key) or `coverage_complete: false`, because the engine could not prove it is safe.

## 3. Write the worklist
Write the project IDs marked **delete** to repo-root `reports/worklist.<account-slug>.txt` (one
per line; format in `$SKILL/templates/worklist.txt`). Echo the final list to the user. Note
any `recycle_keys` choices separately for a later key-security pass.

## Done
Report the worklist contents (or that none were selected). A non-empty worklist is ready to
purge.
