# Ephemeral Cloud Composer (Composer 3) Lifecycle

## Overview

This repo implements an **ephemeral Cloud Composer lifecycle** using:

- Cloud Run services
- Cloud Scheduler jobs
- Composer snapshots
- Secret Manager

The lifecycle follows a **fixed-delay orchestration model**:

```text
create → load snapshot → trigger DAG → DAG deletes environment
```
This approach minimizes cost while ensuring reproducibility through snapshots.

This README focuses on:

- deployment strategy
- required infrastructure
- step-by-step commands
- operational considerations

### Architecture

#### Components

- `create-service` → Creates Composer environment
- `load-service` → Loads environment snapshot
- `trigger-service` → Triggers DAG
- `delete-service` → Deletes Composer environment
- `Cloud Scheduler` → Orchestrates lifecycle
- `Composer snapshot` →	Defines Composer environment state

#### Lifecycle Flow

```text id="flow1"
Cloud Scheduler
    ↓
create-service (06:00)
    ↓
(wait ~35 min)
    ↓
load-service (06:35)
    ↓
(wait ~40 min)
    ↓
trigger-service (07:15)
    ↓
Airflow DAG runs
    ↓
delete-service (from DAG)
```
---

## Deployment Strategy

The system uses a snapshot-driven immutable environment model:

1. Build a baseline Composer environment
2. Configure:
- DAGs
- Variables
- Secret Manager backend
- Python dependencies
3. Validate pipelines
4. Create snapshot
5. Use snapshot for all ephemeral runs

---
## Prerequisites

### 1. Enable APIs

```bash
gcloud services enable \
  composer.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com
```
### 2. Set environment variables

```bash
PROJECT_ID="your-project"
REGION="your-region"
ENV_NAME="bp-composer-ephemeral"
PIPELINE_BUCKET="gs://your-pipeline-bucket" # Pipeline artifacts
COMPOSER_BUCKET="gs://your-composer-bucket" # Composer environment and dags
COMPOSER_SA="<your-composer-service-account>"
COMP_SA="<your-compute-service-account>"
ALLOWED_SNAPSHOT_PREFIX="$PIPELINE_BUCKET/composer-snapshots"
```

## Steps

### 1. Create a Composer Bucket

Benefits:

- stable DAG location
- no bucket churn
- easier debugging

Constraint:
- only one active environment at a time


```bash
gcloud storage buckets create "$BUCKET" --location="$REGION"
```

### 2. Deploy Cloud Run Services

**Create service**

Creates environment with:

- Airflow image version
- workloads config
- service account
- custom bucket

```bash
gcloud run deploy bp-composer-create \                    
  --source ./deployment/create-service \ 
  --project "$PROJECT_ID" \                   
  --region "$REGION" \         
  --service-account "$COMPOSER_SA" \
  --no-allow-unauthenticated \
  --set-env-vars PROJECT_ID="$PROJECT_ID",
    REGION="$REGION",
    COMPOSER_ENV_NAME="$ENV_NAME",
    NODE_SERVICE_ACCOUNT="$COMP_SA",
    IMAGE_VERSION="composer-3-airflow-2.10.5-build.32",
    COMPOSER_BUCKET="$BUCKET"
```

**Load service**

- restores snapshot
- validates snapshot path
- asynchronous


```bash
 gcloud run deploy bp-composer-load \                      
  --source ./deployment/load-service \                
  --project "$PROJECT_ID" \                   
  --region "$REGION" \         
  --service-account "$COMPOSER_SA" \                                      
  --no-allow-unauthenticated \
  --set-env-vars \
    PROJECT_ID="$PROJECT_ID", \
    REGION="$REGION", \
    COMPOSER_ENV_NAME="$ENV_NAME", \
    ALLOWED_SNAPSHOT_PREFIX="$ALLOWED_SNAPSHOT_PREFIX"
```

**Trigger service**

- fetches Airflow URI dynamically
- triggers DAG via REST API

```bash
gcloud run deploy bp-composer-trigger \
  --source ./trigger-service \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --service-account "$COMPOSER_SA" \
  --no-allow-unauthenticated \
  --set-env-vars \
    PROJECT_ID="$PROJECT_ID",\
    REGION="$REGION",\
    COMPOSER_ENV_NAME="$ENV_NAME",\
    DAG_ID="biodiv_pipelines_dag_gcloud_service"
```

**Delete service**

- deletes environment
- idempotent
- called from DAG

```bash
gcloud run deploy bp-composer-delete \
  --source ./deployment/delete-service \
  --service-account "$COMP_SA" \
  --no-allow-unauthenticated \
  --region "$REGION" \
  --set-env-vars \
    PROJECT_ID="$PROJECT_ID", \
    REGION="$REGION", \
    COMPOSER_ENV_NAME="$ENV_NAME"
```

### 3. Create Boostrap Composer Environment

Locate your create-service url and run it manually:

```bash
CREATE_URL=$(gcloud run services describe bp-composer-create \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --format='value(status.url)')

curl -X POST "$CREATE_URL" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"
```
Run this and wait until the environment is ready: 

```bash
gcloud composer environments describe "$ENV_NAME" \
  --location "$REGION" \
  --project "$PROJECT_ID" \
  --format="value(state)"
```
This should return: `RUNNING`

### 4. Configure Secret Manager Backend

Your secrets should be stored in Secret Manager first.

```bash
gcloud composer environments update "$ENV_NAME" \
  --location "$REGION" \
  --project "$PROJECT_ID" \
  --update-airflow-configs \
secrets.backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend,\
secrets.backend_kwargs='{"project_id":"'"$PROJECT_ID"'","variables_prefix":"gbdp","connections_prefix":"airflow-connections","sep":"_"}'
```

Grant access to the Composer service account:

```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$COMPOSER_SA" \
  --role="roles/secretmanager.secretAccessor"
```

### 5. Upload DAGs & Dependencies

```bash
gsutil cp -r ./airflow/dags/* "$COMPOSER_BUCKET/dags/"
gsutil cp -r .airflow/biodiv_airflow/* "$COMPOSER_BUCKET/dags/biodiv_airflow/"

```
### 6. Set Airflow Variables

You can use AIRFLOW UI to set variables or use the following command and repeat for each variable:

```bash
gcloud composer environments run "$ENV_NAME" \
  --location "$REGION" \
  variables set -- biodiv_gcp_project "$PROJECT_ID"

gcloud composer environments run "$ENV_NAME" \
  --location "$REGION" \
  variables set -- biodiv_bucket "$BUCKET"
```

### 7. Validate the DAGs

Run the non-delete DAG:

- confirm success
- confirm Dataflow jobs run correctly

### 8. Create Cloud Scheduler jobs

#### Create Scheduler Service Account

```bash
gcloud iam service-accounts create bp-comp-ephemeral-scheduler
```

#### Grant invoker permissions

Repeat for all services.

```bash
gcloud run services add-iam-policy-binding bp-composer-create \
  --region "$REGION" \
  --member="serviceAccount:bp-comp-ephemeral-scheduler@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

### 9. Get services URLs

```bash
CREATE_URL=$(gcloud run services describe bp-composer-create \
  --region "$REGION" \
  --format='value(status.url)')

LOAD_URL=$(gcloud run services describe bp-composer-load \
  --region "$REGION" \
  --format='value(status.url)')

TRIGGER_URL=$(gcloud run services describe bp-composer-trigger \
  --region "$REGION" \
  --format='value(status.url)')
  
DELETE_URL=$(gcloud run services describe bp-composer-delete \
  --region "$REGION" \
  --format='value(status.url)')
```
The DELETE_URL must be added to the Airflow variables as `delete_service_url`
in order to complete the ephimeral lifecycle setup.

### 10. Create Snapshot

```bash

export SNAPSHOT_PATH="$ALLOWED_SNAPSHOT_PREFIX/baseline-$(date +%Y%m%d-%H%M%S)"

gcloud composer environments snapshots save "$ENV_NAME" \
  --location "$REGION" \        
  --project "$PROJECT_ID" \
  --snapshot-location "$SNAPSHOT_PATH"
```
You can run the DAG biodiv_pipelines_dag_gcloud_service manually to confirm 
that the delete service is working as expected. This will delete the environment.

### 11. Create Scheduler Jobs

The ephemeral composer environment will run the first day of every month with fixed-delay
orchestration for each service.

```text
create  → 0 6 1 * *
load    → 35 6 1 * *
trigger → 15 7 1 * *
```

**Create job**

```bash
gcloud scheduler jobs create http bp-composer-create-job \
  --schedule="0 6 1 * *" \
  --uri="$CREATE_URL" \
  --http-method=POST \
  --oidc-service-account-email="bp-comp-ephemeral-scheduler@$PROJECT_ID.iam.gserviceaccount.com" \
  --oidc-token-audience="$CREATE_URL"
```

**Load job**

```bash
gcloud scheduler jobs create http bp-composer-load-job \
  --schedule="35 6 1 * *" \
  --uri="$LOAD_URL" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body="{\"snapshot_path\":\"$COMPOSER_BUCKET/composer-snapshots/baseline\"}" \
  --oidc-service-account-email="bp-comp-ephemeral-scheduler@$PROJECT_ID.iam.gserviceaccount.com" \
  --oidc-token-audience="$LOAD_URL"
```

**Trigger job**

```bash
gcloud scheduler jobs create http composer-trigger-job \
  --schedule="15 7 1 * *" \
  --uri="$TRIGGER_URL" \
  --http-method=POST \
  --oidc-service-account-email="bp-comp-ephemeral-scheduler@$PROJECT_ID.iam.gserviceaccount.com" \
  --oidc-token-audience="$TRIGGER_URL"
```

#### Test end-to-end lifecycle

```bash
gcloud scheduler jobs run composer-create-job --location "$REGION"
gcloud scheduler jobs run composer-load-job --location "$REGION"
gcloud scheduler jobs run composer-trigger-job --location "$REGION"
```

#### Updating Snapshot

You must update the snapshot path in the load service when adding new DAGs or
updating DAGs. Use the complete snapshot root path.

Example: 
`gs://$ALLOWED_SNAPSHOT_PREFIX/baseline-20260409-185402/<project-id>_<region>_bp-composer-ephemeral_2026-04-09T17-59-17`

```bash
gcloud scheduler jobs update http composer-load-job \
  --message-body "{\"snapshot_path\":\"gs://new-snapshot\"}"
```

### Operational Considerations

#### Timing: Fixed-delay model

- no readiness checks
- relies on time buffers

#### Snapshot integrity

* ensure no test variables remain
- ensure DAG is stable

#### Deletion

- asynchronous
- environment disappears quickly

#### Idempotency

* services tolerate retries
