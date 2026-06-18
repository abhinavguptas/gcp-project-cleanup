# Phase: Plan (dry-run)

Read `docs/iam/<project-id>.yaml` and show every gcloud command that `apply` would run,
grouped by section. Do not execute anything.

## Output format

For each section, show a header and the commands. Mark anything that may need
org-level permissions with ⚠️.

### Custom Roles
```
gcloud iam roles create runDeployer \
  --project=<project_id> \
  --title="Run Deployer" \
  --description="Deploy revisions to existing Cloud Run services only" \
  --permissions="run.configurations.get,run.configurations.list,run.locations.list,\
run.operations.get,run.operations.list,run.revisions.get,run.revisions.list,\
run.routes.get,run.routes.list,run.services.get,run.services.list,run.services.update" \
  --stage=GA
```

### IAM Bindings
One command per member per role. Example pattern:
```
gcloud projects add-iam-policy-binding <project_id> \
  --member="user:<email>" \
  --role="<role>"
```
List every binding that will be created. For the developer custom role:
```
gcloud projects add-iam-policy-binding <project_id> \
  --member="user:<email>" \
  --role="projects/<project_id>/roles/runDeployer"
```
Omit `runDeployer` binding if `env: prod`.

### Org Policies  ⚠️ (requires orgpolicy.policy.set on the project)
```
gcloud resource-manager org-policies enable-enforce \
  constraints/iam.disableServiceAccountKeyCreation --project=<project_id>

gcloud resource-manager org-policies enable-enforce \
  constraints/compute.skipDefaultNetworkCreation --project=<project_id>

gcloud resource-manager org-policies set-policy \
  --project=<project_id> policy-resource-locations.json

gcloud resource-manager org-policies set-policy \
  --project=<project_id> policy-allowed-domains.json

gcloud resource-manager org-policies set-policy \
  --project=<project_id> policy-restricted-services.json
```
If `env: prod`, also show the Cloud Run ingress policy:
```
gcloud resource-manager org-policies set-policy \
  --project=<project_id> policy-run-ingress.json   # is:internal-and-cloud-load-balancing
```
Note: The `set-policy` commands require inline JSON files generated at apply time.

### Budget
```
gcloud billing budgets create \
  --billing-account=<billing_account> \
  --display-name="AI Cap - <project_id>" \
  --budget-amount=<budget_usd>USD \
  --filter-projects="projects/<project_id>" \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0
```

## After showing plan

Tell the user: "This is what `apply` will run. Nothing has been executed yet."
Offer: "Run `/gcp-iam-setup apply <project-id>` to execute."
