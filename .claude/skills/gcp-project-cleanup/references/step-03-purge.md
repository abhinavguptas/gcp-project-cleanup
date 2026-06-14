# Step 03 — purge

## Goal
Delete a confirmed worklist of projects safely, in the background, with a live key-guard.

## Execution rules
- Always dry-run before executing, because the live guard may block a project that gained
  traffic since the scan and the user must see that first.
- Do not add `--allow-keyed` unless the user explicitly accepts deleting idle-keyed projects;
  it never overrides live key traffic.
- A non-active account needs `--quota-project "$QP"` on the delete calls too, because they
  otherwise hit a cross-org quota project and fail.
- State the exact count and ~30-day irreversibility before the confirm, since project
  deletion is destructive.

## Sequence
1. **Assemble inputs.** Worklist = repo-root `reports/worklist.<account-slug>.txt`, or an
   explicit list, or (only if the user says "delete everything recommended") omit `--projects`
   so the report's `delete` recommendations drive it. `ACCT` = that account; `IDS` =
   comma-joined non-comment project IDs.
2. **Dry-run first.**
   ```
   python3 "$SKILL/scripts/delete_projects.py" --account "$ACCT" [--quota-project "$QP"] --projects "$IDS"
   ```
   Show which would delete and which are BLOCKED (and why).
3. **Confirm** with AskUserQuestion (exact count + irreversible after ~30 days).
4. **Execute in the background** on yes (`run_in_background: true`), `--yes` so it does not
   block on the prompt; add `--allow-keyed` only if accepted in step 2:
   ```
   python3 "$SKILL/scripts/delete_projects.py" --account "$ACCT" [--quota-project "$QP"] --projects "$IDS" --execute --yes
   ```
5. **Done.** Read the job output; report deleted / blocked / failed with reasons. Deleted
   projects are marked `deletion_status: deleted` (re-runs skip them). Tell the user they're
   restorable within ~30 days (`gcloud projects undelete`); billing must be re-linked manually.
