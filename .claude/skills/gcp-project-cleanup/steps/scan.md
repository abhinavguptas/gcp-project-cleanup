# Step: scan

Run a decision-grade scan of one GCP account and summarize candidates. `$SKILL` is the
skill folder; the engine is `$SKILL/scripts/scan_projects.py`, runnable from any directory.

## 1. Pick the account
Run `gcloud auth list --format="value(account)"`.
- 0 accounts → tell the user to run `! gcloud auth login`, then stop.
- 1 → use it. >1 → ask which with AskUserQuestion (mark the active one).

Let `ACCT` = chosen account. `--account "$ACCT"` targets it without changing gcloud config.

## 2. Quota project (only if ACCT is NOT the active account)
serviceusage-gated calls (Cloud Asset = the `resources` signal) attribute quota to a
project the account can USE. If `ACCT` != `gcloud config get-value account`, ask the user
for `--quota-project` = any project they OWN in that org, because otherwise resource scans
return `denied`. If `ACCT` IS the active account, omit it (the active project is coherent).

## 3. Run the scan
Resume is the default (re-does only previously-failed signals); `--fresh` forces a full
re-query. Many projects → run with `run_in_background: true` and report on completion.
```
python3 "$SKILL/scripts/scan_projects.py" --account "$ACCT" [--quota-project "$QP"] [--fresh]
```
The report path (repo-root `reports/projects_report.<account-slug>.json`) prints in the first line.

## 4. Summarize
Read the report and present concisely:
- `summary.by_recommendation` + `billable_projects`.
- `scan.signal_coverage` — call out any `denied`/`disabled` signal.
- Each row where recommendation != `keep`, with its one-line reason.
Render using `$SKILL/templates/digest.md`.

## 5. Surface setup gaps (consent before any write)
If `resources` is `disabled`, candidates can't be confirmed empty. Offer the fix and run it
only on explicit OK, because enabling APIs is a write that also adds admin-activity noise:
```
! gcloud services enable cloudasset.googleapis.com --project=PROJECT_ID
```
Same for `usage` (`monitoring.googleapis.com`).

## Done
Report the candidate counts. If there are review/recycle/delete candidates, tell the user
they're ready to triage. If all `keep`, say so plainly — a clean org is a valid result.
