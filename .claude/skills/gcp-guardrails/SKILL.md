---
name: gcp-guardrails
description: Posture-audit and guardrail one GCP project you intend to KEEP (not delete). Read-only scan of services, IAM, keys, budget and org-policy inheritance, then an intent-driven interview that right-sizes roles to each person's goals and emits a reviewed gcloud apply-script. Use to set up a new project safely or audit an existing one for gaps ("guardrail this project", "audit my project's permissions", "set up a new project right"). The deletion counterpart is /gcp-scan + /gcp-triage + /gcp-purge.
user-invocable: true
allowed-tools: Bash, Read, Write, AskUserQuestion
---

# gcp-guardrails

Make ONE kept project safe by reconciling its live posture against a declared baseline
tier plus captured intent (what the project is for, what each person needs to do).
Strictly non-destructive: the only thing this skill writes is a per-project **profile**
and a reviewed **gcloud apply-script** the user runs themselves. Repo root:
`/Users/abhinav/code/gcloud`.

## Voice — the end user is not a GCP expert
- Plain, simple English; short answers. Explain any unavoidable term in 3-4 words in parentheses, because the user does not know GCP jargon.
- Never dump raw gcloud output or JSON at the user — summarize what it means for them.
- After each finding or recommendation, add one **Tip:** line: the usual best practice and why, in one sentence.
- When the situation is ambiguous, add one **Best for you:** line naming the right approach for their case and why.

## Entry: pick the mode
Ask the user (or infer from their request) which mode applies:
- **Setup** — a new project that doesn't exist yet → **[tasks/05-setup.md](tasks/05-setup.md)** (factory: create + stamp the tier baseline). Then jump to step 4.
- **Audit** — an existing project to check for gaps → the loop below.

## The audit loop (BMAD sharded steps — read each task file when you reach it)

```
scan (read-only)  →  interview (intent on gaps only)  →  reconcile (right-size)  →  emit script
```

1. **[tasks/01-scan.md](tasks/01-scan.md)** — pick project + account, run the posture scanner, load the report.
2. **[tasks/02-interview.md](tasks/02-interview.md)** — load profile (or build it), pick the tier, confirm budget values, interview ONLY on gaps and drift.
3. **[tasks/03-reconcile.md](tasks/03-reconcile.md)** — map each person's stated goal to a least-privilege role; resolve every finding to a concrete fix or an accepted exception.
4. **[tasks/04-emit-script.md](tasks/04-emit-script.md)** — write the reviewed `gcloud` apply-script and the updated profile. Never run mutations yourself.

## Core principles (carried from the cleanup skills)

- **Evidence first, then ask.** The scan drives the interview. Ask about intent only where
  config is ambiguous, batch similar gaps, never pre-select a destructive fix.
- **Diff-driven, not improvised.** Desired state = baseline tier + profile intent. The
  interview reconciles actual against that; it does not invent the standard each run.
- **A failed read is not a clean bill.** Every scan section carries an outcome; surface
  `data_gap` findings, never treat "couldn't read" as "nothing wrong".
- **Project-scoped only.** Set project-level budget/quota/IAM; only *verify* org-level
  policy is inherited (read-only). Never mutate org policy from here.
- **Additive + idempotent.** The apply-script only adds/tightens. Flag, never silently
  remove, an existing binding the skill didn't create.

## Assets

- Tier defaults (the single tunable source — budget, kill, quotas, blocked APIs): [config/tiers.yaml](config/tiers.yaml)
- Role archetypes (least-privilege targets): [templates/roles/](templates/roles/)
- Per-project intent capture: [templates/profile.template.yaml](templates/profile.template.yaml), validated by [schemas/profile.schema.json](schemas/profile.schema.json)
- Apply-script skeleton: [templates/apply-script.template.sh](templates/apply-script.template.sh)
- Reviewer checklist: [checklists/posture-checklist.md](checklists/posture-checklist.md)
- Why each finding matters: [data/risk-reference.md](data/risk-reference.md) (keys/budgets/cost facts live in the shared library)
- Scanner: [scripts/audit_project.py](scripts/audit_project.py); GCP transport and key/cost primitives come from the shared [../gcp-py](../gcp-py) library

This skill's own assets and generated artifacts live under this folder; GCP transport and
key/cost primitives come from the sibling `gcp-py` library. Generated artifacts: scan reports
+ emitted apply-scripts in `output/` (gitignored), durable per-project intent in
`profiles/<project>.yaml`. A re-audit diffs live config against the profile and asks only
about *changes*.
