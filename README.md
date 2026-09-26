# ClinicalFlow — Healthcare Claims Intelligence Lakehouse

> Production-grade data lakehouse on **Azure Databricks** covering Unity Catalog, Lakeflow Declarative Pipelines, SCD Type 1 & 2, Delta MERGE upserts, Structured Streaming, Liquid Clustering, Databricks Asset Bundles (dev → staging → prod), Foundation Model APIs, and Vector Search — healthcare claims domain.

![Databricks](https://img.shields.io/badge/Databricks-Premium-FF3621?logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Databricks-0078D4?logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-003366?logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![DAB](https://img.shields.io/badge/DAB-dev%20%7C%20staging%20%7C%20prod-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Overview

ClinicalFlow processes **150M+ synthetic Medicare claims** (CMS SynPUF) across a full medallion architecture — Bronze ingestion via Autoloader, Silver transformation with SCD Type 1 & 2, Gold serving with Liquid Clustered aggregates, and a GenAI layer that generates plain-English risk narratives for care managers via Foundation Model APIs.

The project mirrors enterprise patterns used in large US payer organisations and is deployed across three environments (dev / staging / prod) using Databricks Asset Bundles with GitHub Actions CI/CD.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                       │
│  CMS SynPUF CSVs (all 20 samples)     Simulated ER event stream    │
│  + PySpark synthetic inflation         (Python producer script)     │
└──────────────┬──────────────────────────────┬──────────────────────┘
               │ Autoloader (cloudFiles)       │ Structured Streaming
               ▼                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  BRONZE  (Lakeflow Streaming Tables + DQ Expectations)             │
│  claims_raw            er_admissions_raw         claims_quarantine  │
│  Autoloader ingest     Event ingest              Failed DQ rows     │
└──────────────┬──────────────────────────────┬──────────────────────┘
               │ APPLY CHANGES INTO            │ Windowed aggregation
               │ (SCD Type 1 + SCD Type 2)     │ + MERGE upsert
               ▼                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  SILVER  (Lakeflow Materialized Views + CDF enabled)               │
│  claims_fact (Delta MERGE)    patient_dim (SCD Type 2)             │
│  provider_dim (SCD Type 1)    er_admissions_silver                 │
│  diagnosis_dim (AI-enriched)                                       │
└──────────────┬──────────────────────────────┬──────────────────────┘
               │ Aggregations                  │ Near real-time rollup
               ▼                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  GOLD  (Liquid Clustered, Photon, Predictive I/O)                  │
│  provider_performance    patient_risk_score    claims_trend         │
│  cost_by_diagnosis       er_realtime_dashboard network_adequacy     │
│  patient_risk_narratives (LLM-generated)                           │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
           ┌───────────────┴────────────────┐
           ▼                                ▼
  Foundation Model API             Vector Search Index
  (Llama 3.3 70B — risk           (BGE-Large embeddings
   narrative generation)           on diagnosis narratives)
```

---

## Dimensional Model

**Gold layer uses a Kimball Star Schema — specifically a Galaxy / Constellation Schema** (two fact tables sharing conformed dimensions).

```
              [dim_date]         [dim_patient — SCD Type 2]
                  │                        │
                  │                        │
[dim_claim_flags]─┤                        │
(Junk dim)        │                        │
                  └──────┬─────────────────┘
                         │
              ┌──────────┼──────────┐
              │    fact_claims      │──── [dim_provider — SCD Type 1]
              │    150M rows        │
              │    5 FKs            │──── [dim_diagnosis — AI-enriched]
              └──────────┬──────────┘
                         │
              ┌──────────┴──────────┐
              │  fact_er_admissions │  ← Galaxy schema (shares 4 dims)
              │  15M rows           │
              └─────────────────────┘
                    │
              [dim_procedure — Static, 10K CPT codes]
```

| Table | Type | Rows | SCD |
|---|---|---|---|
| `fact_claims` | Fact | 150M | — |
| `fact_er_admissions` | Fact | 15M | — |
| `dim_patient` | Dimension | ~6M | Type 2 |
| `dim_provider` | Dimension | 450K | Type 1 |
| `dim_date` | Conformed Dim | 1,826 | Static |
| `dim_diagnosis` | Dimension | 72K | Static + AI-enriched |
| `dim_procedure` | Dimension | 10K | Static |
| `dim_claim_flags` | Junk Dim | 8 | Static |

---

## Key Concepts Covered

| Category | Concepts |
|---|---|
| **Unity Catalog** | 3-level namespace, external location, storage credential, column masking (PHI), row-level filters, table tags, data lineage |
| **Lakeflow Pipelines** | Streaming tables, materialized views, DQ expectations (`expect`, `expect_or_drop`, `expect_or_fail`), quarantine pattern, APPLY CHANGES INTO |
| **SCD** | Type 1 (provider_dim — overwrite), Type 2 (patient_dim — full history with `__START_AT/__END_AT/__IS_CURRENT`) |
| **Delta Lake** | MERGE upsert (deduplication), Time Travel, Change Data Feed (CDF), Schema Evolution, OPTIMIZE, VACUUM |
| **Streaming** | Autoloader (cloudFiles), Structured Streaming with watermarking, Trigger.AvailableNow, checkpointing |
| **Optimization** | Liquid Clustering, Photon, Predictive I/O, AQE, broadcast joins, auto-optimize, auto-compact |
| **DAB** | `databricks.yml` with dev / staging / prod targets, parameterized variables, service principal for prod |
| **CI/CD** | GitHub Actions: PR → staging deploy, merge to main → prod deploy |
| **Workflows** | 4-job orchestration, file arrival trigger, multi-task with dependencies |
| **GenAI** | Foundation Model APIs (pay-per-token), Vector Search, AI_QUERY() SQL function, RAG pattern |
| **Governance** | Secrets (Key Vault-backed scope), cluster policies, service principal, table ACLs |

---

## Gold Layer — Business Metrics

### `provider_performance`
Denial rate, avg claim amount, adjusted claims count, avg processing days, out-of-network rate — used by network management and medical directors.

### `patient_risk_score`
Total spend YTD, predicted spend next 90 days, chronic condition count, ER visit count, readmission flag, care gap count, risk tier (LOW / MEDIUM / HIGH / CRITICAL) — used by care managers for daily outreach prioritisation.

### `cost_by_diagnosis`
Total cost per ICD-10 category per quarter, YoY cost change %, preventable ER visit count — used by actuarial for rate-setting.

### `er_realtime_dashboard`
Admission count per department per 1-hour window, avg severity, peak hour flag — near real-time, refreshed via Structured Streaming.

### `claims_trend`
Monthly claims volume, approved/denied amounts, avg adjudication days, MLR estimate — executive and finance dashboards.

### `patient_risk_narratives` *(GenAI)*
Plain-English care manager alerts generated by Llama 3.3 70B via Foundation Model API — e.g. *"Member has 3 chronic conditions, 4 ER visits YTD, $48K spend. Recommend immediate care coordination outreach."*

### `network_adequacy`
Active provider count, members-per-provider ratio, avg distance to provider — regulatory CMS adequacy filing.

---

## Project Structure

```
clinicalflow/
├── databricks.yml                        # DAB bundle root
├── variables.yml                         # Environment variables
│
├── resources/
│   ├── pipelines/
│   │   └── lakeflow_pipeline.yml         # Lakeflow pipeline resource
│   ├── jobs/
│   │   ├── ingest_job.yml                # File arrival trigger → pipeline
│   │   ├── streaming_job.yml             # ER admissions streaming
│   │   ├── maintenance_job.yml           # Weekly OPTIMIZE + VACUUM
│   │   └── genai_job.yml                 # Daily FMAPI enrichment
│   └── clusters/
│       └── job_cluster_policy.yml        # Cost-capped cluster policy
│
├── src/
│   ├── pipelines/
│   │   ├── pipeline_batch.py             # Bronze + Silver batch (Autoloader → SCD)
│   │   ├── pipeline_streaming.py         # ER admissions streaming path
│   │   ├── scd_transforms.py             # APPLY CHANGES INTO (SCD1 + SCD2)
│   │   ├── gold_aggregations.py          # Gold layer tables
│   │   └── expectations.py              # Shared DQ expectation constants
│   │
│   ├── notebooks/
│   │   ├── 01_unity_catalog_setup.py     # UC catalogs, schemas, policies
│   │   ├── 02_data_generation.py         # 150M row synthetic data bootstrap
│   │   ├── 03_delta_time_travel.py       # Time travel + CDF demo
│   │   ├── 04_optimization.py            # Liquid Clustering, OPTIMIZE, VACUUM
│   │   └── 05_ai_query_demo.py           # AI_QUERY() SQL demo
│   │
│   └── genai/
│       ├── fmapi_enrichment.py           # Foundation Model API batch job
│       ├── vector_search_setup.py        # Vector Search index creation
│       └── rag_query.py                  # RAG Q&A on diagnosis data
│
├── data/
│   └── icd10_codes.csv                   # Reference ICD-10 code list (72K codes)
│
├── .github/
│   └── workflows/
│       └── deploy.yml                    # PR → staging, main → prod
│
├── tests/
│   ├── test_dq_expectations.py
│   ├── test_scd_logic.py
│   └── test_merge_dedup.py
│
└── docs/
    ├── architecture.md
    └── star_schema.md
```

---

## Compute Configuration

### Interactive Cluster (`clinicalflow-dev-cluster`)

Used for notebook development, Unity Catalog setup, and data exploration.

| Setting | Value |
|---|---|
| **Cluster name** | `clinicalflow-dev-cluster` |
| **Policy** | Unrestricted ⚠️ (see note below) |
| **Databricks Runtime** | 15.4 LTS (Scala 2.12, Spark 3.5.0) |
| **Node type** | `Standard_D4ads_v6` (4 cores, 16 GB RAM) |
| **Mode** | Single node |
| **Photon** | Disabled (enable for production workloads) |
| **Auto-terminate** | 15 minutes |
| **Estimated cost** | ~1 DBU/h |

> **⚠️ Why Unrestricted policy (not Personal Compute)?**
> The Personal Compute policy restricts the node type list to a fixed set of VM families — the Ddsv6 family (which includes `Standard_D4ads_v6`) is **not included** in that list. You must select **Unrestricted** to see and select Ddsv6 nodes.

> **⚠️ Why not Serverless?**
> The Serverless credential (`adb_clinicalflow`) is scoped only to workspace-managed storage. It **cannot access** `stclinicalflow` (the ADLS Gen2 account backing `clinicalflow_dev` catalog external locations). The interactive cluster uses a managed identity with **Storage Blob Data Contributor** on `stclinicalflow`, so it works.

### Azure vCPU Quota — Required Setup

New Azure PAYG subscriptions ship with **0 quota** for almost all VM families. You must request quota before the cluster will start.

**What to request:**

| Field | Value |
|---|---|
| **Quota family** | Standard Ddsv6 Family vCPUs |
| **Region** | East US 2 |
| **New limit** | 8 cores (covers one 4-core node + buffer) |

**How to request (auto-approved, instant):**

1. Azure Portal → **Quotas** → **Compute**
2. Filter region: **East US 2**, search: **Ddsv6**
3. Click the row → **New Quota Request** → enter **8** → Submit
4. Auto-approved within seconds — no support ticket needed

> **Note:** DSv2 is End of Life (Not Adjustable). FXmdsv2 nodes are not included in the Databricks node list even if quota is granted. Ddsv5 requires a support ticket on Basic plan. **Ddsv6 auto-approves** — use this family.

### Job Clusters (Automated Workflows)

Job clusters are created on-demand by Databricks Workflows and terminated when the job completes.

| Setting | Value |
|---|---|
| **Node type** | `Standard_DS3_v2` (4 cores, 14 GB RAM) |
| **Autoscale** | 1 – 4 workers |
| **Spot strategy** | `SPOT_WITH_FALLBACK_AZURE` |
| **Runtime** | 15.4 LTS |

> Job clusters use `Standard_DS3_v2` (DSv2 family) which has broader availability; quota for DSv2 may already exist on your subscription. If not, request **Standard DSv2 Family vCPUs** (16 cores, East US 2) the same way as above.

---

## Catalog Setup — Known Quirks

### Storage Credential (`UNAUTHORIZED_ACCESS`)

The auto-created workspace credential `adb_clinicalflow` is **metastore-only** — Databricks hard-restricts it to the UC system storage path. Using it for external locations on `stclinicalflow` produces:

```
[UNAUTHORIZED_ACCESS] The credential 'adb_clinicalflow' is a workspace default credential
that is only allowed to access data in the following paths:
'abfss://unity-catalog-storage@dbstoragek27ajb4toues2.dfs.core.windows.net/...'
```

**Fix:** Create a dedicated Access Connector (`ac-clinicalflow`) in Azure, assign it **Storage Blob Data Contributor** on `stclinicalflow`, create a storage credential `clinicalflow_adls` backed by it, and recreate external locations using `clinicalflow_adls`. Full steps in [SETUP.md → Step 7](./SETUP.md#step-7--create-access-connector--storage-credential--external-locations).

### `TABLE_DOES_NOT_EXIST` with UUID error

If you see:

```
[TABLE_DOES_NOT_EXIST.RESOURCE_DOES_NOT_EXIST]
Table '<UUID>' does not exist.
```

This happens when the external location's storage credential doesn't have access to the catalog's managed storage path — the catalog is returning its internal UUID when it can't resolve the storage path. Fix the storage credential first (above), then retry `CREATE TABLE`.

---

## Databricks Edition Required

> ⚠️ **Azure Databricks Premium tier is required.** Standard tier and Databricks Community Edition will not work for this project.

| Feature | Community Edition | Standard | Premium |
|---|---|---|---|
| Unity Catalog | ✗ | ✗ | ✅ |
| Lakeflow / DLT | ✗ | ✗ | ✅ |
| APPLY CHANGES INTO | ✗ | ✗ | ✅ |
| Foundation Model APIs | ✗ | ✗ | ✅ |
| Vector Search | ✗ | ✗ | ✅ |
| Photon | ✗ | ✗ | ✅ |
| Cluster Policies | ✗ | ✗ | ✅ |
| Databricks Workflows | ✗ | ✅ | ✅ |
| Databricks Secrets | ✗ | ✅ | ✅ |
| Multi-node clusters | ✗ | ✅ | ✅ |

**Databricks Community Edition** (community.cloud.databricks.com) is free but single-node only and lacks Unity Catalog, DLT/Lakeflow, Workflows, FMAPI, and Vector Search — none of the enterprise features in this project will run on it.

**Use Azure Pay As You Go (PAYG)** — no upfront cost, billed only for what you run. Estimated total build cost: ~₹3,500–4,500.

---

## Azure Pay As You Go Setup

> 📖 **Full step-by-step setup guide with actual errors encountered and fixes:** [SETUP.md](./SETUP.md)

Quick reference — 12 steps to go from zero to a running workspace:

| Step | What you do | Guide |
|---|---|---|
| 1 | Create Azure account (PAYG) | [Step 1 →](./SETUP.md#step-1--create-azure-account) |
| 2 | Set a $40/month cost alert | [Step 2 →](./SETUP.md#step-2--set-a-cost-alert) |
| 3 | Create resource group `rg-clinicalflow` | [Step 3 →](./SETUP.md#step-3--create-a-resource-group) |
| 4 | Create ADLS Gen2 storage + 3 containers | [Step 4 →](./SETUP.md#step-4--create-adls-gen2-storage-account) |
| 5 | Create Databricks Premium (Hybrid) workspace | [Step 5 →](./SETUP.md#step-5--create-azure-databricks-workspace) |
| 6 | Verify Unity Catalog is auto-enabled | [Step 6 →](./SETUP.md#step-6--verify-unity-catalog-auto-enabled) |
| 7 | Assign IAM role + create 3 external locations | [Step 7 →](./SETUP.md#step-7--assign-iam-role--create-external-locations) |
| 8 | Create catalog via UI (select `clinicalflow_dev_loc`) | [Step 8 →](./SETUP.md#step-8--create-unity-catalog-catalogs--schemas) |
| 9 | Generate Personal Access Token | [Step 9 →](./SETUP.md#step-9--generate-a-personal-access-token-pat) |
| 10 | Configure Databricks CLI | [Step 10 →](./SETUP.md#step-10--configure-databricks-cli) |
| 11 | Clone repo + DAB deploy (dev/staging/prod) | [Step 11 →](./SETUP.md#step-11--clone-repo-and-deploy-with-dab) |
| 12 | Bootstrap 150M synthetic claims | [Step 12 →](./SETUP.md#step-12--bootstrap-synthetic-data) |

> 💡 **Estimated cost:** ~₹3,500–4,500 total for a full dev build. See [cost control tips](./SETUP.md#cost-control-tips).

---

## Data Source

**CMS Synthetic Public Use Files (SynPUF)** — 2.3 million synthetic Medicare beneficiaries, publicly available, zero PII, public domain.

- Download: https://www.cms.gov/data-research/statistics-trends-reports/medicare-claims-synthetic-public-use-files
- All 20 DE samples used: ~100M carrier claims base
- PySpark inflation to reach 150M fact rows for scale demonstration

---

## CI/CD Pipeline

```
Pull Request opened
       │
       ▼
  databricks bundle validate
       │
       ▼
  databricks bundle deploy --target staging
       │
       ▼
  Run DQ test suite (pytest)
       │
       ▼
  PR approved → merge to main
       │
       ▼
  databricks bundle deploy --target prod
  (runs as service principal: clinicalflow-prod-sp)
```

---

## License

MIT © [Jayanth Dolai](https://www.linkedin.com/in/jayanth-dolai-7b115213a/)
