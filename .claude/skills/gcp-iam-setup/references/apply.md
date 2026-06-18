# Phase: Apply

Read `docs/iam/<project-id>.yaml` and execute gcloud commands section by section.
Show each command before running it. Stop and explain on any error — never skip silently.

## Pre-flight checks

```bash
# Confirm project is accessible
gcloud projects describe {{ project_id }} --format="value(lifecycleState)"

# Confirm billing is linked
gcloud billing projects describe {{ project_id }} --format="value(billingEnabled)"

# Confirm caller identity
gcloud config get-value account
```

If billing is not enabled, link it before proceeding:
```bash
gcloud billing projects link {{ project_id }} --billing-account={{ billing_account }}
```

## Section 1 — Enable required APIs

These APIs must be active before any resources can be created.
Run all at once:

```bash
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  aiplatform.googleapis.com \
  cloudbuild.googleapis.com \
  firebase.googleapis.com \
  firebaseauth.googleapis.com \
  pubsub.googleapis.com \
  cloudtasks.googleapis.com \
  cloudscheduler.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  cloudtrace.googleapis.com \
  --project={{ project_id }}
```

Add service APIs for any enabled optional bundles:
- `cloud_sql`: `sqladmin.googleapis.com`
- `redis_memorystore`: `redis.googleapis.com`
- `firebase_hosting`: `firebasehosting.googleapis.com`
- `eventarc`: `eventarc.googleapis.com`
- `alloydb`: `alloydb.googleapis.com`

## Section 2 — Create custom roles

Check if role exists first to make this idempotent:

```bash
gcloud iam roles describe runDeployer --project={{ project_id }} 2>/dev/null \
  && echo "exists" || echo "missing"
```

If missing, create:
```bash
gcloud iam roles create runDeployer \
  --project={{ project_id }} \
  --title="Run Deployer" \
  --description="Deploy revisions to existing Cloud Run services only" \
  --permissions="run.configurations.get,run.configurations.list,run.locations.list,run.operations.get,run.operations.list,run.revisions.get,run.revisions.list,run.routes.get,run.routes.list,run.services.get,run.services.list,run.services.update" \
  --stage=GA
```

If it exists, update permissions to keep it current:
```bash
gcloud iam roles update runDeployer \
  --project={{ project_id }} \
  --permissions="run.configurations.get,run.configurations.list,run.locations.list,run.operations.get,run.operations.list,run.revisions.get,run.revisions.list,run.routes.get,run.routes.list,run.services.get,run.services.list,run.services.update"
```

## Section 3 — IAM bindings

### Owner
```bash
gcloud projects add-iam-policy-binding {{ project_id }} \
  --member="user:{{ owner_email }}" \
  --role="roles/owner"
```

### Project PM
Bind each predefined role. Run one command per role:
```bash
gcloud projects add-iam-policy-binding {{ project_id }} \
  --member="user:{{ pm_email }}" \
  --role="{{ role }}"
```
Roles to bind for PM (from baseline):
`roles/run.developer`, `roles/cloudfunctions.developer`, `roles/datastore.user`,
`roles/storage.objectAdmin`, `roles/artifactregistry.writer`, `roles/cloudbuild.builds.editor`,
`roles/secretmanager.admin`, `roles/aiplatform.user`, `roles/pubsub.admin`,
`roles/cloudtasks.admin`, `roles/cloudscheduler.admin`, `roles/firebase.developAdmin`,
`roles/logging.viewer`, `roles/monitoring.viewer`, `roles/cloudtrace.user`,
`roles/errorreporting.viewer`, `roles/billing.viewer`, `roles/iam.securityReviewer`

### Developers
For each developer email, bind each predefined role:
```bash
gcloud projects add-iam-policy-binding {{ project_id }} \
  --member="user:{{ dev_email }}" \
  --role="{{ role }}"
```
Predefined roles: `roles/datastore.user`, `roles/storage.objectUser`,
`roles/artifactregistry.writer`, `roles/cloudbuild.builds.editor`,
`roles/secretmanager.secretAccessor`, `roles/aiplatform.user`,
`roles/pubsub.subscriber`, `roles/pubsub.publisher`,
`roles/firebase.viewer`, `roles/logging.viewer`, `roles/monitoring.viewer`,
`roles/cloudtrace.user`, `roles/errorreporting.viewer`

Custom role (UAT only — skip if env is prod):
```bash
gcloud projects add-iam-policy-binding {{ project_id }} \
  --member="user:{{ dev_email }}" \
  --role="projects/{{ project_id }}/roles/runDeployer"
```

In prod, bind `roles/run.viewer` instead.

### Optional bundle extra roles
If `cloud_sql` enabled: add `roles/cloudsql.editor` to PM, `roles/cloudsql.client` to developers.
If `redis_memorystore` enabled: add `roles/redis.editor` to PM, `roles/redis.viewer` to developers.
If `firebase_hosting` enabled: add `roles/firebasehosting.admin` to PM, `roles/firebasehosting.viewer` to developers.
If `eventarc` enabled: add `roles/eventarc.admin` to PM, `roles/eventarc.viewer` to developers.
If `alloydb` enabled: add `roles/alloydb.admin` to PM, `roles/alloydb.client` to developers.

## Section 4 — Org Policies

⚠️ Requires `orgpolicy.policy.set` permission on the project (included in `roles/owner`).
If a command fails with PERMISSION_DENIED, tell the user which constraint failed and
suggest they ask their Org Admin to apply it, then continue with the rest.

### Boolean constraints (enforce = on/off)
```bash
gcloud resource-manager org-policies enable-enforce \
  constraints/iam.disableServiceAccountKeyCreation \
  --project={{ project_id }}

gcloud resource-manager org-policies enable-enforce \
  constraints/compute.skipDefaultNetworkCreation \
  --project={{ project_id }}
```

### List constraints (allowed/denied values)
Write a temporary JSON policy file, apply it, then delete the file.

Resource locations:
```bash
cat > /tmp/policy-locations.json << 'EOF'
{
  "constraint": "constraints/gcp.resourceLocations",
  "listPolicy": {
    "allowedValues": ["in:{{ region }}-locations"]
  }
}
EOF
gcloud resource-manager org-policies set-policy /tmp/policy-locations.json \
  --project={{ project_id }}
rm /tmp/policy-locations.json
```

Allowed IAM domains:
Note: this constraint requires the Workspace **Customer ID** (`is:C01abc23`), not the domain
string. Use the `customer_id` resolved during seed. If it is blank (no GCP org), skip this
policy and tell the user.
```bash
cat > /tmp/policy-domains.json << 'EOF'
{
  "constraint": "constraints/iam.allowedPolicyMemberDomains",
  "listPolicy": {
    "allowedValues": ["is:{{ customer_id }}"]
  }
}
EOF
gcloud resource-manager org-policies set-policy /tmp/policy-domains.json \
  --project={{ project_id }}
rm /tmp/policy-domains.json
```

Restricted services (build the denied list from baseline, minus any un-blocked bundles):
```bash
cat > /tmp/policy-services.json << 'EOF'
{
  "constraint": "constraints/serviceusage.restrictedServices",
  "listPolicy": {
    "deniedValues": [
      "compute.googleapis.com",
      "container.googleapis.com",
      "sqladmin.googleapis.com",
      "redis.googleapis.com",
      "file.googleapis.com",
      "bigquery.googleapis.com",
      "dataproc.googleapis.com",
      "composer.googleapis.com",
      "notebooks.googleapis.com",
      "datafusion.googleapis.com",
      "ml.googleapis.com"
    ]
  }
}
EOF
```
Before writing, remove any service from `deniedValues` that corresponds to an enabled
optional bundle (e.g. if `cloud_sql` is enabled, remove `sqladmin.googleapis.com`).

```bash
gcloud resource-manager org-policies set-policy /tmp/policy-services.json \
  --project={{ project_id }}
rm /tmp/policy-services.json
```

### Prod-only org policy (skip if env is uat)
Restrict Cloud Run ingress to internal + load balancer (from `prod_delta` in baseline):
```bash
cat > /tmp/policy-ingress.json << 'EOF'
{
  "constraint": "constraints/run.allowedIngress",
  "listPolicy": {
    "allowedValues": ["is:internal-and-cloud-load-balancing"]
  }
}
EOF
gcloud resource-manager org-policies set-policy /tmp/policy-ingress.json \
  --project={{ project_id }}
rm /tmp/policy-ingress.json
```

## Section 5 — Budget

Create budget:
```bash
gcloud billing budgets create \
  --billing-account={{ billing_account }} \
  --display-name="AI Cap - {{ project_id }}" \
  --budget-amount={{ budget_usd }}USD \
  --filter-projects="projects/{{ project_id }}" \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0
```

Check if a budget already exists to avoid duplicates:
```bash
gcloud billing budgets list \
  --billing-account={{ billing_account }} \
  --format="table(name,displayName)" \
  | grep "AI Cap - {{ project_id }}"
```
If one exists, ask the user: "A budget already exists — update it or skip?"

## Section 6 — Firebase (optional)

Skip this section if `firebase_linked: false` in the project YAML.

Check if Firebase is already linked to avoid re-running:
```bash
firebase projects:list 2>/dev/null | grep {{ project_id }} || echo "not linked"
```

If not linked:
```bash
firebase projects:addfirebase {{ project_id }}
```

This is a one-time command. It initialises Firebase on the GCP project and creates
the default Storage bucket (`{{ project_id }}.firebasestorage.app`).

Requires the Firebase CLI: `npm install -g firebase-tools`
Requires Owner or Editor on the project — run as the project owner, not a developer.

After running, confirm in the summary table as `Firebase linked ✓`.

## Members — patching an existing project

When adding a single developer or PM to an already-provisioned project:

1. Read the existing `docs/iam/<project-id>.yaml`
2. Ask for the new member's email and role tier (developer / pm)
3. Add the member to the YAML file
4. Run only the IAM binding commands for that member (skip everything else)

## After apply

Show a summary table:
| Section | Status |
|---|---|
| APIs enabled | ✓ |
| Custom roles | ✓ created / ✓ already exists |
| IAM bindings | ✓ N bindings applied |
| Org policies | ✓ / ⚠️ N failed (list them) |
| Budget | ✓ created / ✓ already exists |
| Firebase | ✓ linked / — skipped (firebase_linked: false) |

Tell the user: "Project {{ project_id }} is provisioned. Developers can now
deploy to Cloud Run services once the services are created by the PM or Owner."
