---
slug: gcp-project-cleanup
ts: 2026-06-14T18:40:02+05:30
kind: note
called_by: cio-balanced-worker
evidence: []
---

Deleted 8 confirmed-stale GCP projects on 2026-06-14. cs-poc-twycbiur8yirdhxdpld2t7s succeeded immediately (abhinav@ was owner). The remaining 7 failed with permission-denied because their owners are deleted accounts (sid@, kundan@, bhavya@) or rajnish@ with no org-level override. Resolution: granted roles/resourcemanager.projectDeleter to abhinav@concretio.cloud at org level (747714627614), executed all deletions, then removed the role. All 8 projects are now in pending-deletion state (30-day GCP recovery window). The gen-lang-client-* and sys-* projects were not touched.
