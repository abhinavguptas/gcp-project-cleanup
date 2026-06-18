# Compass AI UAT — Provisioning & Developer Onboarding

**Project:** `compass-ai-uat` | **Region:** `us-central1` | **Date:** 2026-06-18

---

## Team

| Person | Email | Role |
|---|---|---|
| Abhinav Gupta | abhinav@concret.io | Owner |
| Chitrank | takchitrank@concret.io | PM / Tech Lead |
| Amit Kumar | amitkumar@concret.io | Developer |

---

## 1. Provisioning Deviations

Three items from the original provisioning request were handled differently. No developer workflow is blocked.

| Requested | What Was Done | Why | Your Action |
|---|---|---|---|
| JSON key file `firebase-service-account-dev.json` | No key file created | `constraints/iam.disableServiceAccountKeyCreation` org policy blocks key creation. JSON keys are long-lived and a leak risk. | Use Application Default Credentials locally. Cloud Run authenticates automatically. See §3. |
| Enable `firebaseauth.googleapis.com` | Not manually enabled | Internal Firebase service — activates automatically on Firebase init, cannot be enabled via `gcloud`. | No action needed. Firebase Auth works via `identitytoolkit.googleapis.com`, which is enabled. |
| 5 org policy constraints at project level | 4 of 5 applied; `serviceusage.restrictedServices` skipped | That specific constraint is unavailable via API in this org. IAM roles already block developers from enabling new APIs. | No action needed. |

---

## 2. Who Owns What

| Capability | Amit (Dev) | Chitrank (PM) | Notes |
|---|---|---|---|
| Deploy new revision to Cloud Run | YES | YES | |
| Create or delete a Cloud Run service | NO | YES | |
| Read and write Firestore data | YES | YES | |
| Read and write Cloud Storage objects | YES | YES | |
| Create or delete Storage buckets | NO | YES | |
| Push / pull Artifact Registry packages | YES | YES | `compass-ai-npm` repo |
| Read secrets at runtime | YES | YES | |
| Create, rotate, or delete secrets | NO | YES | |
| Trigger Cloud Build | YES | YES | |
| Call Vertex AI / Gemini models | YES | YES | |
| Manage Firebase Auth users | NO | YES | |
| View logs and metrics | YES | YES | |
| View billing costs | NO | YES | |
| Enable new GCP APIs | NO | NO | Owner only |
| Modify IAM policies | NO | NO | Owner only |
| Create service accounts | NO | NO | Owner only |

---

## 3. Role Reference

### Amit Kumar — Developer

| Role | What it allows |
|---|---|
| `roles/datastore.user` | Read and write Firestore documents |
| `roles/storage.objectUser` | Read and write objects in existing buckets; no bucket management |
| `roles/artifactregistry.writer` | Push and pull NPM packages from `compass-ai-npm` |
| `roles/secretmanager.secretAccessor` | Read secret values at runtime; cannot create or rotate |
| `roles/aiplatform.user` | Call Vertex AI and Gemini model endpoints |
| `roles/cloudbuild.builds.editor` | Trigger and view Cloud Build jobs |
| `roles/logging.viewer` | View application and infrastructure logs |
| `roles/monitoring.viewer` | View metrics and dashboards |
| `roles/firebase.viewer` | View Firebase project config (read-only); auth flows go via SDK |
| `projects/compass-ai-uat/roles/runDeployer` | Deploy new revisions to existing Cloud Run services; cannot create or delete services |

### Chitrank — PM / Tech Lead

| Role | What it allows |
|---|---|
| `roles/run.developer` | Create, update, and delete Cloud Run services and jobs |
| `roles/datastore.user` | Read and write Firestore documents |
| `roles/storage.objectAdmin` | Full object control in Cloud Storage; limited bucket metadata |
| `roles/artifactregistry.writer` | Push and pull packages from Artifact Registry |
| `roles/secretmanager.admin` | Create, rotate, and delete secrets |
| `roles/aiplatform.user` | Call Vertex AI and Gemini model endpoints |
| `roles/cloudbuild.builds.editor` | Trigger and manage Cloud Build jobs |
| `roles/logging.viewer` | View logs |
| `roles/monitoring.viewer` | View metrics |
| `roles/billing.viewer` | View project costs; cannot change billing account |
| `roles/iam.securityReviewer` | Read all IAM policies; cannot modify |
| `roles/firebase.developAdmin` | Full Firebase dev access including Auth user management |

---

## 4. Service Account

| Field | Value |
|---|---|
| Account | `compass-ai-backend@compass-ai-uat.iam.gserviceaccount.com` |
| Roles | `roles/firebase.admin`, `roles/aiplatform.user`, `roles/firebaseauth.admin` |
| Key file | None — uses Workload Identity |

When a Cloud Run service is deployed with this service account attached, GCP injects credentials automatically. No key file or environment variable needed.

---

## 5. Local Dev Setup (No Key File)

```bash
# 1. Authenticate your personal account
gcloud auth application-default login

# 2. Set the project
gcloud config set project compass-ai-uat

# 3. Do NOT set GOOGLE_APPLICATION_CREDENTIALS — leave it unset
```

The Firebase Admin SDK and `google-auth-library` both detect Application Default Credentials automatically. Your local session uses your own `@concret.io` account permissions, which is sufficient for development.

```js
// Node.js — no key file path needed
const admin = require('firebase-admin')
admin.initializeApp() // picks up ADC automatically
```

```python
# Python — same pattern
import firebase_admin
firebase_admin.initialize_app()  # picks up ADC automatically
```

---

## 6. Enabled APIs

| API | Purpose |
|---|---|
| `aiplatform.googleapis.com` | Vertex AI / Gemini models |
| `firestore.googleapis.com` | Firestore database |
| `storage.googleapis.com` | Cloud Storage |
| `firebase.googleapis.com` | Firebase platform |
| `firebasestorage.googleapis.com` | Firebase Storage |
| `identitytoolkit.googleapis.com` | Firebase Auth |
| `run.googleapis.com` | Cloud Run |
| `cloudbuild.googleapis.com` | Cloud Build |
| `artifactregistry.googleapis.com` | Artifact Registry |
| `cloudresourcemanager.googleapis.com` | Resource management |
| `logging.googleapis.com` | Cloud Logging |
| `secretmanager.googleapis.com` | Secret Manager |

---

## 7. Infrastructure Summary

| Resource | Details |
|---|---|
| Firestore | Native Mode, `us-central1`, default database |
| Artifact Registry | `compass-ai-npm`, NPM format, `us-central1` |
| Budget | ₹17,000/month, alerts at 50/90/100%, POCs billing account |
| Org policies | Location lock (us-central1), no SA keys, no default VPC, domain restriction (concret.io only) |
