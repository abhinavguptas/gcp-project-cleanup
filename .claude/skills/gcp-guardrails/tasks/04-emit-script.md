# Task 04 — Emit reviewed script + save profile

Goal: produce artifacts the user reviews and runs. This skill NEVER runs mutations itself.

## Write the apply-script
Start from [../templates/apply-script.template.sh](../templates/apply-script.template.sh).
Emit one `gcloud` command per confirmed fix from task 03, grouped by area, each preceded by
a comment naming the finding it resolves. Rules:
- Additive/tightening only. To replace a role, emit the `add-iam-policy-binding` for the new
  role AND a clearly-commented `remove-iam-policy-binding` for the old one — never an
  implicit removal.
- Every destructive line (key delete, role removal) stays commented-out by default with a
  `# REVIEW:` prefix, so the user opts in deliberately.
- Budgets: emit the `gcloud billing budgets create` with the tier amount + thresholds.
- End with a re-audit hint so the user can verify convergence.

Write to `output/apply.<PROJ>.sh` under the skill root. Echo the path and a one-line summary
of what it will change. Do **not** chmod or execute it.

## Save the profile
Write the captured intent to `profiles/<PROJ>.yaml` under the skill root, conforming to
[../schemas/profile.schema.json](../schemas/profile.schema.json). This is the durable
desired-state; the next audit diffs against it and only asks about drift.

## Hand off
Tell the user:
- the high/medium finding counts and what the script fixes,
- that they review `output/apply.<PROJ>.sh` and run it themselves,
- to re-run the skill afterward to confirm a clean posture.
