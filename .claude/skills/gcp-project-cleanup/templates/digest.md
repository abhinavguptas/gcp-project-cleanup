# Digest format — how scan/triage render a report to the user.

`## <account> — <total> projects`

One summary line: `keep N · review N · recycle_keys N · delete N · billable N · data-gaps N`.

Then a table of every row where `recommendation != keep`, deletability order
(delete, recycle_keys, review):

| Project | Rec | Conf | Resources | Usage | Last admin | Keys | Why |
|---|---|---|---|---|---|---|---|

- Usage cell: `N` when `usage.status == ok`, else the status (`denied`/`disabled`/`unknown`).
- Last admin: `activity.last_admin_action` date + principal, or `none`.
- Keys: count, flag `live`/`idle`/`unrestricted` from `credentials.api_keys[].usage`/`risk`.
- Why: join `decision.reasons` + `blockers`.

Call out, in one line each: signals that are `denied`/`disabled` (and the API to enable),
and any API key with `risk: ["unrestricted"]` that has traffic.
