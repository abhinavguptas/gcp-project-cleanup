---
name: gcp-scan
description: Scan a GCP account for project-cleanup candidates. Picks the account, ensures the required APIs are on, runs scan_projects.py to a per-account report, and summarizes keep/review/recycle/delete with the blockers. Use when the user wants to inventory, scan, or refresh cleanup status for a GCP org/account ("scan my projects", "what can I clean up", "refresh the gcp report").
user-invocable: true
allowed-tools: Bash, Read, AskUserQuestion
---

# gcp-scan

Run a decision-grade scan of one GCP account and hand a summary to the user. Wraps
`scan_projects.py`. Work from the repo root (`/Users/abhinav/code/gcloud`).

## 1. Pick the account
Run `gcloud auth list --format="value(account)"`.
- 0 accounts → tell the user to run `! gcloud auth login` and stop.
- 1 account → use it.
- >1 → ask which with AskUserQuestion (the active one marked).

Let `ACCT` = chosen account. Everything below passes `--account "$ACCT"`; this never
changes the user's active gcloud config.

### Quota project (only when ACCT is NOT the active account)
serviceusage-gated calls (Cloud Asset = the `resources` signal) attribute quota to a
project the account must be able to USE. If `ACCT` differs from
`gcloud config get-value account`, the active project belongs to another org and resource
scans will come back `denied`. Ask the user for `--quota-project` = a project they OWN in
that org (it just provides API quota; any owned project works), and pass it below. If
`ACCT` IS the active account, omit it (the active project is already coherent).

## 2. Run the scan
Default is resume (fast, re-does only previously-failed signals). Use `--fresh` only if
the user wants a full re-query. Add `--quota-project "$QP"` when step 1 determined one.

```
python3 scan_projects.py --account "$ACCT" [--quota-project "$QP"]          # resume/refresh
python3 scan_projects.py --account "$ACCT" [--quota-project "$QP"] --fresh  # full re-query
```

A full scan of many projects is long-running — run it with `run_in_background: true` and
report when it finishes. The report is `projects_report.<account-slug>.json` (printed in
the first log line).

## 3. Summarize for the user
Read the report. Present, concisely:
- `summary.by_recommendation` (keep / review / recycle_keys / delete) and `billable_projects`.
- `scan.signal_coverage` — call out any signal that is `denied`/`disabled`.
- The actionable rows (recommendation != keep) with their one-line reason.

## 4. Surface setup gaps (don't auto-fix)
If `resources` coverage is `disabled`, deletion candidates can't be confirmed (can't
prove a project is empty). Tell the user the fix and offer to run it only on explicit OK:
```
! gcloud services enable cloudasset.googleapis.com --project=PROJECT_ID
```
Same pattern for `usage` (`monitoring.googleapis.com`). Enabling APIs is a write and adds
admin-activity noise — get consent first, never enable silently.

## 5. Hand off
If there are review/recycle/delete candidates, suggest `/gcp-triage` to decide on them.
If everything is `keep`, say so plainly — a clean org is a valid result.
