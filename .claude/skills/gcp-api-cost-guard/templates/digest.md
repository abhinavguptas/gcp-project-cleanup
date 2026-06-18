# Bird's-eye digest format

Render the report in this shape. Lead with what breaks soonest.

## URGENT before 2026-06-19
List every unrestricted key (project, key name, calls/window) and every billable project with
NO budget. These are the live leak plus the Gemini cutoff for unrestricted standard keys.

## Per-project table
| Project | Billing | Budget | Keys | Unrestricted | Std (->2026-09) | Top finding |
|---------|---------|--------|------|--------------|-----------------|-------------|

Sort worst-first (urgent, then high). Mark projects with a `data_gap` finding so an unreadable
project is never shown as clean.

## Cost note
Per-key cost shown is call volume + services. GCP exposes no per-key dollars; project/SKU
dollars require a BigQuery billing export.
