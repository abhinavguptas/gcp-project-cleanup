# Phase: Seed

Collect config, fill the baseline template, write `docs/iam/<project-id>.yaml`.

## Step 1 — Collect config

Ask only what's missing. If the user already provided values in their request, use them.
Ask all at once if possible (one AskUserQuestion call, not one per field).

Required fields:
- `project_id` — GCP project ID (e.g. `compass-ai-uat`)
- `project_name` — display name (e.g. `Compass AI UAT`); defaults to `project_id` if omitted
- `env` — `uat` or `prod`
- `region` — default `us-central1` unless user specifies
- `org_domain` — e.g. `concret.io` (display/label only)
- `billing_account` — if unknown, run: `gcloud billing accounts list --format="table(name,displayName)"`
- `budget_usd` — monthly ceiling in USD
- `owner_email` — full project owner
- `pm_email` — tech lead / PM
- `developer_emails` — comma-separated list

Resolve the Workspace Customer ID (required for the IAM domain-restriction policy):
```bash
gcloud organizations list --format="value(owner.directoryCustomerId)"
```
Store the result as `customer_id` (format: `C01abc23`). If the command returns nothing,
the account has no GCP org — skip the `iam.allowedPolicyMemberDomains` policy at apply time
and tell the user why.

## Step 2 — Ask about optional bundles

Show this list and ask which apply to the project:

| Bundle | Use when |
|---|---|
| `cloud_sql` | Project uses PostgreSQL or MySQL |
| `redis_memorystore` | Project needs a shared cache |
| `firebase_hosting` | Static frontend via Firebase Hosting |
| `eventarc` | Event-driven triggers between services |
| `alloydb` | High-performance Postgres-compatible DB |

Default: none enabled. User can say "none" to skip.

## Step 3 — Verify project exists

```bash
gcloud projects describe {{ project_id }} --format="value(name,lifecycleState)"
```

If it doesn't exist, ask: "Project not found — should I create it?"
If yes, proceed to create:
```bash
gcloud projects create {{ project_id }} --name="{{ project_name | default: project_id }}"
gcloud billing projects link {{ project_id }} --billing-account={{ billing_account }}
```

## Step 4 — Write project YAML

Read `docs/iam/baseline-fullstack-ai.yaml`, substitute all `{{ placeholders }}` with
collected values, uncomment selected optional bundles, and write to
`docs/iam/{{ project_id }}.yaml`.

Show a compact summary before writing:
- Project, env, region, domain, budget
- Members (owner / pm / developers)
- Enabled optional bundles
- Predefined roles per role tier (count only, e.g. "Developer: 9 roles + runDeployer custom role")

Confirm: "Ready to save this config?" before writing the file.

## Step 5 — Offer next step

After writing: "Config saved. Run `/gcp-iam-setup apply {{ project_id }}` to provision,
or `/gcp-iam-setup plan {{ project_id }}` to preview the gcloud commands first."
