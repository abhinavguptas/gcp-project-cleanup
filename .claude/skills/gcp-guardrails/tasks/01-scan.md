# Task 01 — Posture scan (read-only)

Goal: produce the evidence the interview reasons over. No mutation.

## Pick the account
Run `gcloud auth list --format="value(account)"`.
- 0 accounts → tell the user to run `! gcloud auth login` and stop.
- 1 account → use it. `>1` → ask which with AskUserQuestion (active one marked).

Let `ACCT` = chosen account. If `ACCT` is NOT the active account, ask for a
`--quota-project` (any project `ACCT` owns) — serviceusage-gated reads (org policies,
budgets) attribute quota to it, same caveat as /gcp-scan.

## Pick the project
If the user named a project, use it. Otherwise list candidates with
`gcloud projects list --account "$ACCT" --format="value(projectId)"` and ask which one.
Let `PROJ` = chosen project ID.

## Run the scanner
```
python3 .claude/skills/gcp-guardrails/scripts/audit_project.py \
  --project "$PROJ" --account "$ACCT" [--quota-project "$QP"]
```
Writes `audit_report.<PROJ>.json` and prints a severity-ranked summary. For a project with
many Cloud Run services or SAs this can take a minute — fine to run foreground.

## Read the report
Load `audit_report.<PROJ>.json`. Note the `findings` list (already ranked high→info) and
any `data_gap` findings — those are sections that could not be read (denied/disabled), and
must be surfaced to the user, never treated as "clean".

Proceed to [02-interview.md](02-interview.md).
