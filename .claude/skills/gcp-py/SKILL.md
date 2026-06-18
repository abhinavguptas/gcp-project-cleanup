---
name: gcp-py
description: Internal shared library for the gcp-* skills (GCP transport, API-key and billing primitives, risk reference). Not a workflow; imported by sibling skills' scripts.
user-invocable: false
disable-model-invocation: true
---

# gcp-py

Shared library for the gcp-* skills. Not invoked as a skill: its scripts are imported by
sibling skills via a sys.path shim. The folder must stay a sibling of its consumers under
`.claude/skills/`.

## Modules

| Module | Provides |
|---|---|
| `scripts/gcp.py` | Transport: `run`, `set_account`, `current_account`, `account_slug`, `repo_root`, `report_path`, `access_token`, `monitoring_sum`, `classify_stderr`, outcome constants `OK/DENIED/DISABLED/TIMEOUT/ERROR`. Every call returns an explicit `outcome` so a failed read is never read as empty. |
| `scripts/keys.py` | API-key + SA-key primitives: `list_keys`, `classify_key`, `key_usage`, `make_key_record` (canonical key-record shape), `sa_user_keys`, `GEMINI_APIS`. |
| `scripts/cost.py` | Billing/budget reads: `project_billing`, `project_budgets`. |
| `data/risk-reference.md` | Shared facts: Gemini key deadlines, no-per-key-dollar limit, budgets-not-caps, SA-key risk. |

## Import shim

Put at the top of a consumer script (sibling skill under `.claude/skills/`):

```python
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "gcp-py", "scripts"))
import gcp, keys, cost
```

## Contract rules

1. **One-directional** — consumers import gcp-py; gcp-py imports no feature skill, to prevent cycles.
2. **Capability and fact only** — no skill-specific policy (budget amounts, role lists), decision
   logic, or flow lives here, because policy two skills would set differently belongs to each skill.
3. **Pure** — functions over gcloud/curl only; no `AskUserQuestion`, no model invocation, since the
   library is imported, never run as a skill.

Changing a function signature or the key-record shape is a breaking change for every consumer;
update them together.
