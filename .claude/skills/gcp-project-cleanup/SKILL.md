---
name: gcp-project-cleanup
description: End-to-end GCP project cleanup for an account/org — scan for obsolete projects, triage candidates with evidence, and delete safely with a live API-key guard. Single entry point that routes to the right phase. Handles "scan my gcp projects", "what can I clean up", "triage/review projects", "delete these projects", "gcp cleanup", "find idle projects".
user-invocable: true
allowed-tools: Bash, Read, Write, AskUserQuestion
---

# gcp-project-cleanup

## Purpose
Own the whole GCP project-cleanup flow from one entry point: scan an account for obsolete
projects, triage candidates with evidence, and delete safely. Routes to the right phase by
intent and state. Step playbooks, schema, and templates live under this folder; the engine
imports the shared `gcp-py` library (a sibling skill) for GCP transport and key/cost primitives.

```
<skill>/
  scripts/      scan_projects.py · delete_projects.py   (the engine; imports ../gcp-py)
  references/   step-01-scan.md · step-02-triage.md · step-03-purge.md
  schemas/      projects_report.v2.json                          (report contract)
  templates/    worklist.txt · digest.md
```

Let `SKILL` = this skill's directory ("Base directory for this skill" on invocation; from
repo root, `.claude/skills/gcp-project-cleanup`). Run the engine as
`python3 "$SKILL/scripts/<name>.py"` from the repo root.

## Preconditions
- `gcloud` installed and authenticated (`gcloud auth list` shows at least one account);
  if none, tell the user to run `! gcloud auth login` and stop.
- Working directory is the repo root, since runtime outputs resolve to repo-root `reports/`.

## Inputs
- `account` (optional) — credentialed account to target; default is the active gcloud account.
- `quota_project` (required only when `account` is NOT the active account) — any project that
  account owns, for API quota.
- `intent` (optional) — scan / triage / purge; if absent, inferred from state below.

## Outputs
- `reports/projects_report.<account-slug>.json` — the scan report (gitignored).
- `reports/worklist.<account-slug>.txt` — the confirmed delete list from triage (gitignored).

## Voice — the end user is not a GCP expert
- Plain, simple English; short answers. Explain any unavoidable term in 3-4 words in parentheses, because the user does not know GCP jargon.
- Never dump raw gcloud output or JSON at the user — summarize what it means for them.
- After each finding or recommendation, add one **Tip:** line: the usual best practice and why, in one sentence.
- When the situation is ambiguous, add one **Best for you:** line naming the right approach for their case and why.

## Routing — pick the phase, read that step file, follow it
Detect state first (outputs live in repo-root `reports/`):
```
ls reports/projects_report.*.json   # which accounts have a report
ls reports/worklist.*.txt           # any pending delete worklist
```

| Intent (words) | Phase | Step file |
|---|---|---|
| scan, inventory, refresh, "what's idle", "what can I clean up" | scan | `references/step-01-scan.md` |
| triage, review, "what should I delete", decide | triage | `references/step-02-triage.md` |
| purge, delete, execute, "remove these" | purge | `references/step-03-purge.md` |
| unclear → no report yet | scan | `references/step-01-scan.md` |
| unclear → report exists, no worklist | triage | `references/step-02-triage.md` |
| unclear → worklist exists | purge | `references/step-03-purge.md` |

Read the chosen step file and follow it exactly. Re-invoking resumes at whatever phase the
state implies.

## References (load on demand)
- `schemas/projects_report.v2.json` — the report's JSON Schema; read it when explaining or
  consuming the report.
- `templates/digest.md` — the human-summary format the scan/triage steps render.
- `templates/worklist.txt` — the worklist format purge consumes.

## Hard rules (apply in every phase)
- A **non-active** account needs `--quota-project <a project it owns>` because serviceusage-
  gated calls (Cloud Asset = the `resources` signal) attribute quota to core/project, so a
  cross-org active project gets denied; the active account needs none since its project is coherent.
- Never recommend or execute delete on an `unknown`/`denied` signal, because the engine cannot
  prove the project is unused; `denied`/`disabled` is not a confirmed zero.
- Deletion is dry-run by default and re-checks API keys LIVE before each delete, so a key that
  gained traffic since the scan still hard-blocks the deletion.
