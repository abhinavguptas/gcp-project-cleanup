#!/usr/bin/env bash
# gcp-guardrails apply-script for project: __PROJECT__
# Generated __DATE__. REVIEW every line before running. This skill does NOT run it for you.
#
# Conventions:
#   - Additive/tightening only.
#   - Destructive lines (role/key removal) are commented with `# REVIEW:` — opt in deliberately.
#   - Run with the account that owns the project; nothing here touches org-level policy.
set -euo pipefail

PROJECT="__PROJECT__"

# ---- IAM: right-size to intent -------------------------------------------------------
# (one block per person; new least-privilege role added, old basic role removal staged)
# gcloud projects add-iam-policy-binding "$PROJECT" \
#   --member="user:NAME@concret.io" --role="roles/run.developer"
# REVIEW: gcloud projects remove-iam-policy-binding "$PROJECT" \
#   --member="user:NAME@concret.io" --role="roles/editor"

# ---- Budget + kill-switch ------------------------------------------------------------
# gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" \
#   --display-name="$PROJECT-budget" --budget-amount=__LIMIT__USD \
#   --filter-projects="projects/$PROJECT" \
#   --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
#   (wire to Pub/Sub + disable-billing function when enforce_kill=true — see .claude/skills/gcp-py/data/risk-reference.md)

# ---- API keys: restrict ---------------------------------------------------------------
# gcloud services api-keys update KEY_ID --api-target=service=generativelanguage.googleapis.com
# REVIEW: gcloud services api-keys delete KEY_ID   # if the key is unused/unsafe

# ---- Cloud Run: cap autoscaling -------------------------------------------------------
# gcloud run services update SERVICE --project="$PROJECT" --region=REGION \
#   --max-instances=__MAX__ --min-instances=0

# ---- Verify --------------------------------------------------------------------------
echo "Re-run the audit to confirm a clean posture:"
echo "  python3 .claude/skills/gcp-guardrails/scripts/audit_project.py --project $PROJECT"
