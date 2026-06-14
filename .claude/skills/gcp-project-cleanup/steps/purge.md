# Step: purge

Execute deletion for a confirmed worklist via `$SKILL/scripts/delete_projects.py`, which
re-checks API keys LIVE before each delete (a key with traffic hard-blocks). Project
deletion is reversible only within ~30 days.

## 1. Get the worklist + account
- Worklist: repo-root `reports/worklist.<account-slug>.txt`, an explicit list the user gives,
  or — only if the user says "delete everything recommended" — omit `--projects` so the
  report's `delete` recommendations drive it.
- `ACCT` = the account the worklist came from. `IDS` = comma-separated project IDs.
- If `ACCT` is not the active gcloud account, also pass `--quota-project "$QP"` (a project
  the account owns), because otherwise the delete calls hit a cross-org quota project and fail.

## 2. Dry-run first, always
```
python3 "$SKILL/scripts/delete_projects.py" --account "$ACCT" [--quota-project "$QP"] --projects "$IDS"
```
Show which would delete and which are BLOCKED (and why). If the guard blocks something the
user expected to delete, explain it (e.g. a key gained traffic since the scan). Do NOT add
`--allow-keyed` unless the user explicitly accepts that risk; it never overrides live traffic.

## 3. Confirm, then execute in the background
Ask the user to confirm with AskUserQuestion, stating the exact count and that it is
irreversible after ~30 days. On yes, run with `run_in_background: true` and `--yes` (so it
does not block on the interactive prompt):
```
python3 "$SKILL/scripts/delete_projects.py" --account "$ACCT" [--quota-project "$QP"] --projects "$IDS" --execute --yes
```
Add `--allow-keyed` only if the user accepted idle-keyed deletions in step 2.

## Done
Read the job output and report deleted / blocked / failed with reasons. Deleted projects are
marked `deletion_status: deleted` in the report, so re-runs skip them. Tell the user they are
restorable within ~30 days (`gcloud projects undelete`); billing must be re-linked manually.
