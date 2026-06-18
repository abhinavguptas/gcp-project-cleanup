#!/usr/bin/env bash
# gcp-api-cost-guard apply-script. Generated __DATE__. REVIEW every line before running.
# This skill does NOT run it for you.
#
# Conventions:
#   - Additive/tightening only (restrict keys, create budgets).
#   - Destructive lines (key delete, breaking migration) are commented `# REVIEW:` - opt in.
#   - Run with the account that owns each project; nothing here touches org-level policy.
set -euo pipefail

# ---- API keys: restrict to Gemini (+ optional app restriction) -----------------------
# gcloud services api-keys update KEY_UID --api-target=service=generativelanguage.googleapis.com
#   (append --allowed-referrers=... or --allowed-ips=... for an application restriction)
# REVIEW: gcloud services api-keys delete KEY_UID   # if the key is unused/unsafe
# REVIEW: migrate standard keys to auth keys in AI Studio / Cloud Console before 2026-09

# ---- Budgets: cap billable projects --------------------------------------------------
# gcloud billing budgets create --billing-account=BILLING_ACCOUNT \
#   --display-name="PROJECT-budget" --budget-amount=AMOUNTUSD \
#   --filter-projects="projects/PROJECT" \
#   --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0

# ---- Verify --------------------------------------------------------------------------
echo "Re-run the scan to confirm a clean posture:"
echo "  python3 .claude/skills/gcp-api-cost-guard/scripts/scan_keys.py --account ACCOUNT"
