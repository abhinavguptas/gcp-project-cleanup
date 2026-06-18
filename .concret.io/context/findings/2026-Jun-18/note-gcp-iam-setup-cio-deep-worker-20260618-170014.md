---
slug: gcp-iam-setup
ts: 2026-06-18T17:00:14+05:30
kind: note
called_by: cio-deep-worker
evidence:
  - "baseline-fullstack-ai.yaml L154 firebase.viewer; capability matrix L71 Firebase Auth test flows YES"
  - "SKILL.md routing table L37-44"
---

Lower-severity observations (NOT applied, for review):

MED capability mismatch: developer gets roles/firebase.viewer (read-only) but capability matrix claims "Firebase Auth - test flows YES". firebase.viewer cannot exercise Auth test flows. Either downgrade the matrix claim or grant a data-plane Auth role. Baseline decision needed.

LOW routing ambiguity: a request like "setup IAM and apply for project X" matches both the seed row (setup) and apply row. First-match-by-reading-order picks seed, which is the safe default, but SKILL.md does not state the precedence rule explicitly. Consider adding "first matching row wins" note.

LOW: SKILL.md routing references "references/apply.md SS Members" for add-developer; the actual heading is "## Members - patching an existing project". Anchor is loose but the section exists, so navigation works.

LOW: seed.md Step 3 project-create path runs `gcloud projects create ... --name="{{ project_name }}"` but project_name is never collected in Step 1 (only project_id). Either collect project_name or default it to project_id.

LOW: idempotency of IAM bindings is fine (add-iam-policy-binding is idempotent). Org-policy set-policy is also idempotent (full replace). No re-run hazard found.
