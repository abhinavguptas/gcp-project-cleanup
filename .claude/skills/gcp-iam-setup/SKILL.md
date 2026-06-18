---
name: gcp-iam-setup
description: >
  Seed, plan, and apply the GCP IAM baseline for a Full Stack + AI project.
  Handles "setup IAM for project", "apply IAM baseline", "add developer to project",
  "seed IAM config", "provision new GCP project access".
user-invocable: true
model: sonnet
allowed-tools: Bash, Read, Write, AskUserQuestion
---

# gcp-iam-setup

## Purpose
Apply the IAM baseline (`docs/iam/baseline-fullstack-ai.yaml`) to a GCP project.
Three phases: **seed** (collect config, write project YAML), **plan** (show what
gcloud commands will run), **apply** (execute them with confirmation).

All project YAMLs live at `docs/iam/<project-id>.yaml` relative to repo root.
Baseline template lives at `docs/iam/baseline-fullstack-ai.yaml`.

## Preconditions
- `gcloud` authenticated: run `gcloud auth list` — must show at least one active account.
- Caller has Owner or Editor on the target project.
- For org policies: caller needs `orgpolicy.policy.set` at project level
  (Owner role includes this). If it fails, surface the exact error and tell the user
  to ask their Org Admin to apply the org-policy block manually.

## Routing

Detect intent and state, then read the matching reference file.

```
ls docs/iam/<project-id>.yaml   # project YAML exists?
```

First matching row wins.

| Words in request | State | Phase | Reference |
|---|---|---|---|
| seed, new project, setup, init, configure | any | seed | references/seed.md |
| plan, preview, dry-run, show commands | YAML exists | plan | references/plan.md |
| apply, run, execute, provision | YAML exists | apply | references/apply.md |
| add developer, add member, add user | YAML exists | patch-member | references/apply.md §"Members — patching" |
| no match | no YAML | seed | references/seed.md |
| no match | YAML exists | ask user: plan or apply? | — |

Always read the baseline before starting:
```bash
cat docs/iam/baseline-fullstack-ai.yaml
```

## Voice
- Plain English, short answers. User is not a GCP expert.
- Never dump raw gcloud output — summarize what it means.
- After each apply step, confirm success or explain the error in plain terms.
- One **Tip:** line after any non-obvious recommendation.
