# Step 02 — triage

## Goal
Walk the user through the scan candidates and produce a confirmed delete worklist.

## Execution rules
- Never pre-select delete for a project with a blocker (live traffic / recent admin / live
  key) or `coverage_complete: false`, because the engine could not prove it is safe.
- Don't bundle a project that has any blocker or data_gap into a batch question, because each
  needs an explicit human choice.
- Present evidence in plain English; on "deep-dive" you may run live `gcloud` reads, but
  summarize them — never paste raw output.

## Sequence
1. **Locate the report.** List `reports/projects_report.*.json` (repo root). If more than one,
   ask which account. Load it. Candidates = `decision.recommendation` in
   {`review`, `recycle_keys`, `delete`}. None → say so and stop.
2. **Triage each candidate**, ordered `delete` → `recycle_keys` → `review` (oldest
   `activity.last_admin_action` first). Show a compact evidence block: recommendation +
   confidence + reasons + blockers + data_gaps; `signals`; resources total +
   `coverage_complete`; API keys with per-key `calls_in_window`/`risk`; last admin action
   (date + principal); billing enabled.
3. **Ask** with AskUserQuestion: delete / recycle keys / keep / skip / deep-dive.
   - deep-dive → print the full record (and, if asked what's deployed, live `gcloud asset
     search-all-resources` / `services list`), then re-ask.
   - Batch only obviously-similar clean rows (e.g. several empty `sys-*`).
4. **Write the worklist.** Put the IDs marked **delete** in repo-root
   `reports/worklist.<account-slug>.txt` (one per line; format in
   `$SKILL/templates/worklist.txt`). Echo the list. Note any `recycle_keys` choices
   separately for a later key-security pass.
5. **Done.** Report the worklist (or that none were selected). A non-empty worklist is ready
   to purge.
