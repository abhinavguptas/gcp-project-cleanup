# Step 01 — Scan

Goal: produce `reports/keys_report.<account-slug>.json` and show the bird's-eye digest.

1. **Pick the account.** `gcloud auth list --format="value(account)"`. 0 -> tell the user to
   run `! gcloud auth login`, then stop. 1 -> use it. >1 -> AskUserQuestion (mark the active
   one). Let `ACCT` = chosen account.
2. **Quota project** — only if `ACCT` != `gcloud config get-value account`: ask for any project
   the account OWNS, because cross-org reads attribute quota to a project the account can use.
   Skip when `ACCT` is the active account.
3. **Scope.** If the user named one project, set `--project <id>`; otherwise account-wide (all
   projects the account can list).
4. **Run the scan** from the repo root:
   `python3 "$SKILL/scripts/scan_keys.py" --account "$ACCT" [--quota-project QP] [--project PID]`
5. **Render the digest.** Read the report and present it in the `templates/digest.md` shape:
   an "URGENT before 2026-06-19" block first (unrestricted keys, billable projects with no
   budget), then a per-project table. State the cost caveat from `scan.cost_note` verbatim:
   per-key cost is call volume + services; GCP exposes no per-key dollars.
6. **Hand off.** If any urgent/high findings exist, offer to review and fix them.
