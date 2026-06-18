---
name: gcp-ensure-owner-perms
description: Check IAM permissions for the active account at project, org, and billing scopes, then self-grant any roles needed by gcp-project-cleanup, gcp-guardrails, and gcp-api-cost-guard. Use when a skill returns PERMISSION_DENIED or before first use of the gcp-* skills. Handles "check my gcp permissions", "why is the scan failing with denied", "grant me the roles I need".
user-invocable: true
allowed-tools: Bash, Read, AskUserQuestion
---

# gcp-ensure-owner-perms

Test the active gcloud account's IAM permissions at project, org, and billing scopes;
self-grant any roles missing for the gcp-* skills. The engine emits grant commands
but never applies them without confirmation.

Let `SKILL` = this skill's directory (`.claude/skills/gcp-ensure-owner-perms`).

## Voice — the end user is not a GCP expert
- Plain, simple English. Explain any GCP term in 3-4 words in parentheses.
- Never dump raw script output — summarize findings and say what they mean.
- After each finding, add one **Tip:** line: the usual best practice, one sentence.

## Preconditions
- `gcloud` installed and authenticated (`gcloud auth list` shows an account); if not,
  tell the user to run `! gcloud auth login` and stop.
- To fix org-level gaps the user needs org admin rights; project and billing fixes
  require admin rights on those resources. Report what requires what.

## Inputs
- `project` (optional) — any GCP project the user owns; defaults to the active
  gcloud project (`gcloud config get-value project`).

## Steps

### 1. Resolve the target project
```bash
gcloud config get-value account
gcloud config get-value project
```
If no project is set, ask: "Which GCP project ID should I use as the test target?
(Any project you own works.)"

### 2. Run the permission check (read-only)
```bash
python3 "$SKILL/scripts/check_perms.py" --project PROJECT_ID
```
Summarize the output in plain English:
- How many permissions are missing, split by scope (project / org / billing).
- Which skills are blocked (gcp-project-cleanup / gcp-guardrails / gcp-api-cost-guard)
  and which are unaffected.
- Never dump the raw script output.

### 3. If no gaps: stop
Tell the user: "All required permissions are in place. The gcp-* skills are ready."

### 4. If gaps exist: ask what to do
Present three options:
- **Fix now** — apply grants immediately
- **Dry run** — preview the `gcloud` commands first, then decide
- **Skip** — show manual steps instead

### 5. Dry run (if chosen)
```bash
python3 "$SKILL/scripts/check_perms.py" --project PROJECT_ID --dry-run
```
Show the `gcloud add-iam-policy-binding` commands in a code block. Ask: "Looks good?
Apply now?" before proceeding to step 6.

### 6. Apply (if confirmed)
```bash
python3 "$SKILL/scripts/check_perms.py" --project PROJECT_ID --fix
```
Report each role granted. For any grant failure, explain the manual fix in plain
English with the exact `gcloud` command the user can run with a higher-privilege
account.

## Hard rules
- Never run `--fix` without explicit user confirmation, because IAM changes affect
  access controls and require an intentional user decision.
- If `test-iam-permissions` fails for a scope, report it as unknown — never claim
  the permissions are present when the check itself could not complete.
- Failed grants at org or billing scope must produce the exact manual `gcloud` command
  to run, not a vague "contact your admin" message.
