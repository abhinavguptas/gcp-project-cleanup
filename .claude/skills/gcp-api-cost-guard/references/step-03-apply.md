# Step 03 — Apply

Goal: write `reports/apply-keys.<account-slug>.sh` from the recorded choices. Do NOT run it.

1. Start from `templates/apply-script.template.sh`.
2. For each recorded choice, emit one line. Additive lines run as-is; destructive ones are
   commented `# REVIEW:` so an unattended run cannot break a live integration.
   - Restrict to Gemini:
     `gcloud services api-keys update KEY_UID --api-target=service=generativelanguage.googleapis.com`
   - Add app restriction (when the user gave referrers/IPs): append `--allowed-referrers=...`
     or `--allowed-ips=...` to the update line.
   - Set / add budget (use the project's `billing_account` from the report):
     `gcloud billing budgets create --billing-account=BA --display-name="PID-budget" --budget-amount=AMOUNTUSD --filter-projects="projects/PID" --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0`
   - Delete key: `# REVIEW: gcloud services api-keys delete KEY_UID`
   - Migrate to auth key: `# REVIEW:` note (done in AI Studio / Cloud Console, not scriptable here).
3. Write the file to `reports/apply-keys.<account-slug>.sh`.
4. Tell the user: review every line, run it with the account that owns the project, then re-run
   the scan to confirm a clean posture.
