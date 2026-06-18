# Task 05 — Setup mode (new-project factory)

Goal: a new project born with guardrails already on, so it's never naked. Emits a reviewed
creation script; this skill does not create the project itself.

## Gather inputs (AskUserQuestion)
1. **Tier** — `poc` | `app-dev` | `app-prod` | `shared-ai`. Load that block from
   [../config/tiers.yaml](../config/tiers.yaml).
2. **Name** — derive from the tier's `naming` pattern:
   - poc → `poc-<owner>-<slug>` ; app-dev → `<app>-dev` ; app-prod → `<app>-prod` ;
     shared-ai → `ai-keys-quarantine`. Ask for the missing pieces (owner, slug, app).
3. **Confirm money values** — show the tier's `budget.limit_usd` + `enforce_kill`; let the
   user accept or override. Never apply silently.
4. **People** — who gets access and their goal; map each to a role archetype in
   [../templates/roles/](../templates/roles/), same as the audit interview.
5. **Labels** — fill the tier's `required_labels` (owner, ttl for poc; app, env, owner for apps).

## Emit the creation script
Start from [../templates/create-project.template.sh](../templates/create-project.template.sh).
Fill in: project create + link billing, labels, budget (+ kill wiring if `enforce_kill`),
GPU quota 0, disable `blocked_apis` (or enable `allowed_apis` for shared-ai), Cloud Run
max-instances cap, and the per-person IAM bindings. Write to `output/create.<PROJ>.sh`.

Same safety rules as task 04: additive only, destructive lines commented `# REVIEW:`, no
org-level policy mutation, end with a re-audit hint.

## Save the profile
Write `profiles/<PROJ>.yaml` from [../templates/profile.template.yaml](../templates/profile.template.yaml),
conforming to [../schemas/profile.schema.json](../schemas/profile.schema.json), so the first
real audit diffs against captured intent.

## Hand off
Tell the user the project name, tier, confirmed budget, and that they review and run
`output/create.<PROJ>.sh`, then run audit mode once it exists to verify a clean posture.
