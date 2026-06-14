---
name: gcp-project-cleanup
description: End-to-end GCP project cleanup for an account/org — scan for obsolete projects, triage candidates with evidence, and delete safely with a live API-key guard. Single entry point that routes to the right phase. Use for "scan my gcp projects", "what can I clean up", "triage/review projects", "delete these projects", "gcp cleanup", "find idle projects".
user-invocable: true
allowed-tools: Bash, Read, Write, AskUserQuestion
---

# gcp-project-cleanup

One skill that owns the whole cleanup flow. It is **self-contained**: everything it needs
lives under this folder, so nothing elsewhere in the repo affects it.

```
<skill>/
  scripts/    gcp.py · scan_projects.py · delete_projects.py   (the engine)
  steps/      scan.md · triage.md · purge.md                   (phase playbooks)
  schemas/    projects_report.v2.json                          (report contract)
  templates/  worklist.txt · digest.md                         (output formats)
```

Let `SKILL` = this skill's directory (the "Base directory for this skill" shown on invocation;
from repo root it is `.claude/skills/gcp-project-cleanup`). Run the engine as
`python3 "$SKILL/scripts/<name>.py"`. Runtime outputs are written to **repo-root `reports/`**
(per-account `projects_report.<account>.json` + `worklist.<account>.txt`, gitignored) so the
skill folder stays clean and versioned.

## Routing — pick the phase, then read that step file and follow it

First detect state (outputs live in repo-root `reports/`):
```
ls reports/projects_report.*.json   # which accounts have a report
ls reports/worklist.*.txt           # any pending delete worklist
```

Choose the phase from the user's intent; if intent is unclear, fall back to state:

| Intent (words) | Phase | Step file |
|---|---|---|
| scan, inventory, refresh, "what's idle", "what can I clean up" | **scan** | `steps/scan.md` |
| triage, review, "what should I delete", decide | **triage** | `steps/triage.md` |
| purge, delete, execute, "remove these" | **purge** | `steps/purge.md` |
| unclear → no report yet | scan | `steps/scan.md` |
| unclear → report exists, no worklist | triage | `steps/triage.md` |
| unclear → worklist exists | purge | `steps/purge.md` |

Then **Read the chosen `steps/*.md` and follow it exactly.** Each step hands off to the next
(scan → triage → purge). Re-invoking this skill resumes at whatever phase the state implies.

## References (load on demand)
- `schemas/projects_report.v2.json` — the report's JSON Schema; the contract for any
  downstream reporting/automation. Read it when explaining or consuming the report.
- `templates/digest.md` — the human-summary format the scan/triage steps render.
- `templates/worklist.txt` — the worklist format purge consumes.

## Hard rules (apply in every phase)
- Multi-account: `--account` targets a credentialed account without changing gcloud config.
  A **non-active** account also needs `--quota-project <a project it owns>` (else resource
  scans get denied). The active account needs neither.
- Never recommend/execute delete on an `unknown`/`denied` signal; `denied`/`disabled` ≠ `0`.
- Deletion is dry-run by default and re-checks API keys LIVE; a key with traffic hard-blocks.
