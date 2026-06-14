---
name: gcp-purge
description: Delete a confirmed list of GCP projects in the background, with a live API-key guard and a single explicit confirmation. Use after /gcp-triage to execute the cleanup ("delete these projects", "purge the worklist", "run the deletion"). Deletes 1-N projects; each is re-checked live for API-key traffic before deletion.
user-invocable: true
allowed-tools: Bash, Read, AskUserQuestion
---

# gcp-purge

Execute project deletion for a confirmed worklist. Wraps `delete_projects.py`, which
re-checks API keys LIVE before each delete (a key with traffic hard-blocks). Repo root:
`/Users/abhinav/code/gcloud`. Project deletion is reversible only within ~30 days.

## 1. Get the worklist + account
- Worklist: a `worklist.<account-slug>.txt` from `/gcp-triage`, an explicit list the user
  gives, or — only if the user says "delete everything recommended" — omit `--projects`
  and let the report's `delete` recommendations drive it.
- Account: `ACCT` (the same account the worklist came from).
- Build `IDS` = comma-separated project IDs.
- If `ACCT` is not the active gcloud account, also pass `--quota-project "$QP"` (a project
  the account owns), same as `/gcp-scan` — otherwise the delete calls hit a cross-org
  quota project and fail. Append it to both commands below.

## 2. ALWAYS dry-run first
```
python3 delete_projects.py --account "$ACCT" --projects "$IDS"
```
Show the user the result: which would delete, which are BLOCKED (and why). If the live
guard blocks something the user expected to delete, explain it (e.g. a key gained traffic
since the scan) — do NOT reach for `--allow-keyed` unless the user explicitly accepts that
risk, and note it never overrides live key traffic.

## 3. Confirm, then execute in the background
Ask the user to confirm with AskUserQuestion, stating the exact count and that it's
irreversible after ~30 days. On yes, run in the background (`run_in_background: true`) with
`--yes` (non-interactive) so it doesn't block on a prompt:
```
python3 delete_projects.py --account "$ACCT" --projects "$IDS" --execute --yes
```
Add `--allow-keyed` ONLY if the user accepted idle-keyed deletions in step 2.

## 4. Report
When the background job finishes, read its output and report deleted / blocked / failed
counts with reasons. Deleted projects are marked `deletion_status: deleted` in the report,
so re-runs skip them. Remind the user deletions can be restored within ~30 days via the
Cloud Console (billing must be re-linked manually).
