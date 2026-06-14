# Step 01 — scan

## Goal
Scan one GCP account for cleanup candidates and give the user a plain-English summary.

## Execution rules
- `--account "$ACCT"` targets the account without changing gcloud config.
- A non-active account also needs `--quota-project "$QP"` (a project it owns), because
  serviceusage-gated calls (Cloud Asset = the `resources` signal) attribute quota to
  core/project and a cross-org active project is denied otherwise.
- Enabling an API is a write — get explicit consent first, because it also writes an
  admin-activity audit entry that skews the `activity` signal.
- Summarize per the digest format; never paste raw JSON or gcloud output at the user.

## Sequence
1. **Pick the account.** `gcloud auth list --format="value(account)"`. 0 → tell the user to
   run `! gcloud auth login`, stop. 1 → use it. >1 → AskUserQuestion (mark the active one).
   Let `ACCT` = chosen account.
2. **Quota project**, only if `ACCT` != `gcloud config get-value account`: ask the user for
   `--quota-project` = any project they OWN in that org. Skip if `ACCT` is the active account.
3. **Run the scan.** Resume is default (re-does only previously-failed signals); `--fresh`
   forces a full re-query. Many projects → `run_in_background: true`, report on completion.
   ```
   python3 "$SKILL/scripts/scan_projects.py" --account "$ACCT" [--quota-project "$QP"] [--fresh]
   ```
   Report path (repo-root `reports/projects_report.<account-slug>.json`) prints first.
4. **Summarize** from the report using `$SKILL/templates/digest.md`:
   `summary.by_recommendation` + `billable_projects`; any `denied`/`disabled` in
   `scan.signal_coverage`; each row where recommendation != `keep` with its reason.
5. **Surface setup gaps.** If `resources` is `disabled`, candidates can't be confirmed empty.
   Offer the fix and run it only on explicit OK:
   `! gcloud services enable cloudasset.googleapis.com --project=PROJECT_ID`
   (same for `usage` → `monitoring.googleapis.com`).
6. **Done.** Report candidate counts. If review/recycle/delete candidates exist, tell the user
   they're ready to triage. If all `keep`, say so — a clean org is a valid result.
