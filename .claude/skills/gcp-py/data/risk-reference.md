# GCP risk reference — keys, budgets, cost

Shared reasoning fuel for the gcp-* skills. Terse by design.

## Gemini / API keys (the live exposure)
- Enabling `generativelanguage.googleapis.com` lets EVERY API key in the project call Gemini.
- Cutoffs: **2026-06-19** Gemini rejects **unrestricted standard keys**; **2026-09** rejects all
  **standard** keys. Migrate to **auth keys** (service-account-bound, restricted to the Gemini API,
  default in AI Studio now).
- "Unrestricted" = no API restriction (`apiTargets`). Restrict a needed key:
  `gcloud services api-keys update KEY --api-target=service=generativelanguage.googleapis.com`
  plus an application restriction (referrer/IP).
- gcloud does not expose standard-vs-auth key type; classify on restriction state and flag the gap.

## Cost attribution limits
- GCP has **no per-API-key dollar API**. BigQuery billing export slices cost by project / service /
  SKU / label only (no credential dimension). So per-key spend = call volume + services (Cloud
  Monitoring `request_count` on `credential_id="apikey:<uid>"`); dollars are project/SKU level.

## Budgets are NOT caps
- Budgets only notify and lag by hours. To actually stop spend, wire
  **Budget -> Pub/Sub -> Cloud Function that disables billing** (`projects.updateBillingInfo` with
  empty account). Destructive; reserve for sandboxes, never production.

## Service-account keys
- User-managed (downloadable) SA keys are an exfil + sprawl risk. Prefer Workload Identity
  Federation / short-lived credentials. Org policy `iam.disableServiceAccountKeyCreation` blocks
  new ones (admin-level).
