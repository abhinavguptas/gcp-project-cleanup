#!/usr/bin/env bash
# gcp-guardrails create-script for NEW project: __PROJECT__  (tier: __TIER__)
# Generated __DATE__. REVIEW every line before running. This skill does NOT run it for you.
#
# Born-with-guardrails: budget + kill-switch, GPU 0, blocked/allowed APIs, max-instances cap,
# least-privilege IAM. Additive only; destructive lines commented `# REVIEW:`.
set -euo pipefail

PROJECT="__PROJECT__"
FOLDER="__FOLDER_ID__"            # the tier's folder (sandbox/apps/shared)
BILLING_ACCOUNT="__BILLING_ACCOUNT__"

# ---- Create + link billing + label ----------------------------------------------------
gcloud projects create "$PROJECT" --folder="$FOLDER" \
  --labels=__LABELS__              # e.g. owner=ravi,ttl=30d  (poc)  |  app=x,env=prod,owner=ravi
gcloud billing projects link "$PROJECT" --billing-account="$BILLING_ACCOUNT"

# ---- Budget (+ kill-switch when tier enforce_kill=true) -------------------------------
gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" \
  --display-name="$PROJECT-budget" --budget-amount=__LIMIT__USD \
  --filter-projects="projects/$PROJECT" \
  --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
# enforce_kill=true → also wire Budget→Pub/Sub→disable-billing function (see ../data/risk-reference.md)

# ---- Quotas: hard-block GPU -----------------------------------------------------------
# GPU quota override to 0 (raising requires an explicit request). Set via Console/Quotas API.

# ---- APIs: block the money/key surface (or allow, for shared-ai) ----------------------
# poc / app-* : keep these DISABLED (do not enable):
#   generativelanguage.googleapis.com  apikeys.googleapis.com   → use Vertex per-project
# shared-ai only: gcloud services enable generativelanguage.googleapis.com apikeys.googleapis.com aiplatform.googleapis.com --project="$PROJECT"

# ---- Cloud Run: cap autoscaling (applied when first service deploys) -------------------
# enforce via custom org policy at folder level; or pass --max-instances=__MAX__ on deploy

# ---- IAM: least-privilege per person --------------------------------------------------
# gcloud projects add-iam-policy-binding "$PROJECT" \
#   --member="user:NAME@concret.io" --role="roles/run.developer"

# ---- Verify ---------------------------------------------------------------------------
echo "Audit the new project to confirm a clean posture:"
echo "  python3 .claude/skills/gcp-guardrails/scripts/audit_project.py --project $PROJECT"
