---
slug: gcloud-cleanup
ts: 2026-06-18T16:57:38+05:30
kind: note
called_by: cio-balanced-worker
evidence: []
---

Deleted gws generate-skills artifacts from repo root: `/skills/` (50+ GWS skill dirs duplicating ~/.claude/skills/gws-*) and `docs/skills.md` (auto-generated GWS index, explicitly labelled "Do not edit manually"). Both were untracked (?? in git status). Added `/skills/` and `/docs/skills.md` to `.gitignore` as guardrail against re-generation landing here. `docs/iam/` and `.claude/skills/` untouched.
