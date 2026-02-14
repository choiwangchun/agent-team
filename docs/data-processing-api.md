# Data Processing Backend API

## Goal

Upload heterogeneous Excel/CSV or JSON table data and unify semantically equivalent columns into canonical ontology fields.

Example:

- Company A: `Sex = male/female`
- Company B: `Boy/Girl = boy/girl`
- Unified field: `gender = male/female`

## Architecture

- Upload is **asynchronous**.
- `POST /api/data/upload` or `POST /api/data/table` creates an ingestion job.
- Background queue worker processes jobs and creates datasets.
- You poll `GET /api/jobs/:jobId` until status is `completed`.

## Storage

- If `DATABASE_URL` is set: PostgreSQL persistence is used.
- If `DATABASE_URL` is not set: in-memory storage is used.
- Storage mode check: `GET /api/system/storage`

## Endpoints

### 1) System/Health

- `GET /healthz`
- `GET /ops/metrics`
- `GET /api/system/storage`

### 2) Ontology Fields

#### List fields

`GET /api/ontology/fields`

#### Add/Upsert fields

`POST /api/ontology/fields`

```json
{
  "fields": [
    {
      "key": "department",
      "label": "Department",
      "aliases": ["dept", "team", "부서"],
      "valueAliases": {
        "sales": ["sales", "biz", "영업"],
        "engineering": ["engineering", "dev", "개발"]
      }
    }
  ]
}
```

### 3) Column Mapping Overrides (Manual)

Use this to force mapping for a specific company/schema.

#### List overrides

`GET /api/ontology/overrides?companyName=CompanyA`

#### Upsert override

`POST /api/ontology/overrides`

```json
{
  "companyName": "CompanyC",
  "sourceColumn": "GType",
  "canonicalField": "gender"
}
```

### 4) Ingestion (Async Job)

#### Upload Excel/CSV

`POST /api/data/upload` (multipart/form-data)

Fields:

- `companyName`: string
- `file`: xlsx/xls/csv

Response: `202 Accepted`

```json
{
  "job": {
    "id": "job-id",
    "status": "pending",
    "payload": {
      "companyName": "CompanyA",
      "sourceName": "company_a.csv",
      "rowCount": 200
    }
  }
}
```

#### Upload JSON table

`POST /api/data/table`

```json
{
  "companyName": "CompanyA",
  "sourceName": "api-table",
  "table": [
    { "Name": "Alex", "Sex": "male", "Age": 31 },
    { "Name": "Jamie", "Sex": "female", "Age": 28 }
  ]
}
```

Response: `202 Accepted` with job payload.

### 5) Jobs

#### List jobs

`GET /api/jobs?limit=50`

#### Get one job

`GET /api/jobs/:jobId`

Job statuses:

- `pending`
- `running`
- `completed`
- `failed`

When completed, `job.result.datasetId` is available.

### 6) Datasets

#### List datasets

`GET /api/data/datasets`

#### Dataset detail

`GET /api/data/datasets/:datasetId?rows=20`

### 7) Merge datasets

`POST /api/data/merge`

```json
{
  "datasetIds": ["dataset-id-1", "dataset-id-2"],
  "limit": 5000
}
```

## Quick cURL Flow (Async)

```bash
# 1) Upload
UPLOAD_RESP=$(curl -sS -X POST http://localhost:3000/api/data/upload \
  -F "companyName=CompanyA" \
  -F "file=@company_a.csv")

JOB_ID=$(echo "$UPLOAD_RESP" | jq -r '.job.id')

# 2) Poll job
curl -sS http://localhost:3000/api/jobs/$JOB_ID

# 3) Use datasetId from completed job
DATASET_ID=$(curl -sS http://localhost:3000/api/jobs/$JOB_ID | jq -r '.job.result.datasetId')

# 4) Merge
curl -sS -X POST http://localhost:3000/api/data/merge \
  -H "content-type: application/json" \
  -d "{\"datasetIds\":[\"$DATASET_ID\"],\"limit\":100}"
```

## PostgreSQL Setup

```bash
export DATABASE_URL='postgres://user:password@localhost:5432/saas_dashboard'
npm run dev
```

Tables are auto-created at server startup.

## Current built-in canonical fields

- `gender`
- `name`
- `age`
- `company`
- `email`
