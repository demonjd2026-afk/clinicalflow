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
| 8 | Create catalog via UI (Default Storage) | [Step 8 →](./SETUP.md#step-8--create-unity-catalog-catalogs--schemas) |
| 9 | Generate Personal Access Token | [Step 9 →](./SETUP.md#step-9--generate-a-personal-access-token-pat) |
| 10 | Configure Databricks CLI | [Step 10 →](./SETUP.md#step-10--configure-databricks-cli) |
| 11 | Clone repo + DAB deploy (dev/staging/prod) | [Step 11 →](./SETUP.md#step-11--clone-repo-and-deploy-with-dab) |
| 12 | Bootstrap 150M synthetic claims | [Step 12 →](./SETUP.md#step-12--bootstrap-synthetic-data) |

> 💡 **Estimated cost:** ~₹3,500–4,500 total for a full dev build. See [cost control tips](./SETUP.md#cost-control-tips).

---

## Compute Configuration

> ⚠️ **Do not use Serverless compute for setup notebooks.** Serverless clusters use a restricted workspace default credential (`adb_clinicalflow`) that is scoped only to the internal UC metastore storage. Managed tables in `clinicalflow_dev` are stored in `clinicalflow_dev_loc` (ADLS Gen2 `stclinicalflow`), which requires a regular interactive cluster with the workspace managed identity.

### Interactive Cluster (notebooks 01–05, data generation)

| Setting | Value |
|---|---|
| **Policy** | Personal Compute |
| **Runtime** | `15.4 LTS` (Scala 2.12, Spark 3.5.0) — uncheck Machine Learning |
| **Instance type** | `Standard_D4ds_v5` (16 GB, 4 cores) |
| **Mode** | Single node |
| **Data access** | Unity Catalog (auto-configured) |
| **Estimated cost** | ~1 DBU/h |

> The runtime must be **15.4 LTS (Scala 2.12)** to match `spark_version: 15.4.x-scala2.12` set in `variables.yml`. Do not use the Machine Learning runtime variant — it bundles ML libraries that are unnecessary here and runs on a different DBR.

### Why Serverless Fails

Serverless clusters use an Entra-managed workspace credential (`adb_clinicalflow`) whose storage access is locked to the internal UC metastore path (`abfss://unity-catalog-storage@dbstoragek27ajb4toues2.dfs.core.windows.net/...`). When a catalog is created with a custom external location (e.g. `clinicalflow_dev_loc` pointing to `stclinicalflow`), any `CREATE TABLE` or `saveAsTable` call routes managed table data to that custom storage — which the Serverless credential cannot reach, producing:

```
UNAUTHORIZED_ACCESS: The credential 'adb_clinicalflow' is a workspace default credential
that is only allowed to access data in the following paths:
'abfss://unity-catalog-storage@dbstoragek27ajb4toues2.dfs.core.windows.net/...'
```

A regular interactive cluster inherits the workspace managed identity, which was granted **Storage Blob Data Contributor** on `stclinicalflow` when the external location was set up — so it can write to both storage accounts.

### Job Clusters (DAB-deployed pipelines)

Job clusters are defined in `variables.yml` and `resources/clusters/job_cluster_policy.yml`:

| Setting | Value |
|---|---|
| **Runtime** | `15.4.x-scala2.12` |
| **Driver** | `Standard_DS3_v2` |
| **Workers** | `Standard_DS3_v2`, min 1 / max 4 (autoscale) |
| **Spot policy** | `SPOT_WITH_FALLBACK_AZURE` |
| **On-demand fallback** | Enabled (prevents job failure if spot unavailable) |

---

## Catalog Setup — Known Quirk

The `clinicalflow_dev` catalog **must be created via the Databricks UI**, not SQL. The workspace metastore has no root storage URL configured, so `CREATE CATALOG` without `MANAGED LOCATION` fails with:

```
Metastore storage root URL does not exist.
```

**UI path:** Catalog → + Add → Add a catalog → select `clinicalflow_dev_loc` as storage location → name `clinicalflow_dev` → Create.

Then create schemas in SQL Editor:

```sql
USE CATALOG clinicalflow_dev;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

The notebook `01_unity_catalog_setup.py` assumes the catalog and schemas already exist and will skip catalog creation.

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
