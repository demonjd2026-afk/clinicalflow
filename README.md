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

Follow these steps once to provision the full infrastructure.

### Step 1 — Create Azure Account

1. Go to [portal.azure.com](https://portal.azure.com)
2. Click **Start free** → sign in with a Microsoft/Outlook account or create one
3. Complete identity verification (credit/debit card required — not charged upfront)
4. Select **Pay As You Go** when prompted for a subscription type

> Azure offers a **$200 free credit for 30 days** on new accounts. The entire dev build fits within this credit.

---

### Step 2 — Set a Cost Alert (Do This First)

1. In Azure Portal → search **Cost Management + Billing** → select your subscription
2. Click **Budgets** → **Add**
3. Set amount: **$40** (≈ ₹3,350), period: **Monthly**
4. Add alert at 80% and 100% with your email
5. Click **Create**

This ensures you get an email before spending beyond your budget.

---

### Step 3 — Create a Resource Group

1. Search **Resource groups** → **Create**
2. Subscription: Pay As You Go
3. Resource group name: `rg-clinicalflow`
4. Region: **East US 2** (best Databricks Premium availability)
5. Click **Review + Create** → **Create**

---

### Step 4 — Create ADLS Gen2 Storage Account

1. Search **Storage accounts** → **Create**
2. Resource group: `rg-clinicalflow`
3. Storage account name: `stclinicalflow` *(must be globally unique, lowercase, no hyphens)*
4. Region: **East US 2**
5. Performance: **Standard**
6. Redundancy: **LRS** *(cheapest — sufficient for a portfolio project)*
7. Click **Advanced** tab → under **Data Lake Storage Gen2** → enable **Hierarchical namespace** ✅ *(critical — this is what makes it ADLS Gen2)*
8. Click **Review + Create** → **Create**

After creation, go to the storage account → **Containers** → **+ Container**:
- Create three containers: `clinicalflow-dev`, `clinicalflow-staging`, `clinicalflow-prod`

---

### Step 5 — Create Azure Databricks Workspace

1. Search **Azure Databricks** → **Create**
2. Resource group: `rg-clinicalflow`
3. Workspace name: `adb-clinicalflow`
4. Region: **East US 2**
5. Pricing tier: **Premium (+ Role-based access controls)** ← mandatory
6. Workspace type: **Hybrid** ← select this, not Serverless
   - Hybrid gives you your own ADLS Gen2 storage, custom job clusters, cluster policies, and full Unity Catalog external location support
   - Serverless uses Databricks-managed storage only and does not support custom compute or external locations
7. Click **Review + Create** → **Create**

Wait ~2 minutes for deployment. Then click **Launch Workspace**.

---

### Step 6 — Enable Unity Catalog

Unity Catalog requires a one-time metastore setup at the Azure Databricks account level.

1. Go to [accounts.azuredatabricks.net](https://accounts.azuredatabricks.net)
2. Sign in with the same Azure account
3. Click **Data** → **Create Metastore**
4. Name: `clinicalflow-metastore`
5. Region: **eastus2**
6. ADLS Gen2 path: `abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/metastore`
7. Click **Create**
8. Under **Workspaces** → assign `adb-clinicalflow` to this metastore

---

### Step 7 — Create Storage Credential + External Location

In your Databricks workspace:

1. Go to **Catalog** (left sidebar) → **External Data** → **Credentials** → **Create credential**
2. Select **Azure Managed Identity** or create a **Service Principal** in Azure AD with Storage Blob Data Contributor role on your storage account
3. Enter the application ID and secret
4. Name: `clinicalflow-storage-credential`

Then create External Locations:

1. **External Data** → **External locations** → **Create**
2. Name: `clinicalflow_dev_loc`
3. URL: `abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/`
4. Credential: `clinicalflow-storage-credential`
5. Repeat for `clinicalflow_staging_loc` and `clinicalflow_prod_loc`

---

### Step 8 — Create Unity Catalog Catalogs

Open a notebook in your workspace and run:

```sql
-- Create one catalog per environment
CREATE CATALOG IF NOT EXISTS clinicalflow_dev;
CREATE CATALOG IF NOT EXISTS clinicalflow_staging;
CREATE CATALOG IF NOT EXISTS clinicalflow_prod;

-- Create schemas inside each catalog
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.bronze;
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.silver;
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.gold;
```

---

### Step 9 — Generate a Personal Access Token

1. In Databricks workspace → top-right avatar → **Settings**
2. **Developer** → **Access tokens** → **Generate new token**
3. Name: `clinicalflow-pat`, expiry: 90 days
4. Copy the token immediately — it is shown only once

---

### Step 10 — Install Databricks CLI and Configure

```bash
# Install Databricks CLI (v2)
pip install databricks-cli

# Verify version (needs >= 0.200 for DAB support)
databricks --version

# Configure with your workspace URL and token
databricks configure --token
# Prompt: Databricks Host → https://adb-<id>.azuredatabricks.net
# Prompt: Token → paste your PAT from Step 9
```

---

### Step 11 — Clone Repo and Deploy

```bash
# Clone the repository
git clone https://github.com/demonjd2026-afk/clinicalflow.git
cd clinicalflow

# Validate the DAB bundle
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy --target dev

# Deploy to staging
databricks bundle deploy --target staging

# Deploy to prod (uses service principal)
databricks bundle deploy --target prod
```

---

### Step 12 — Bootstrap Synthetic Data

```bash
# Generate 150M synthetic claims in dev
databricks bundle run data_generation_job --target dev

# Verify row counts in notebook
# SELECT COUNT(*) FROM clinicalflow_dev.bronze.claims_raw  → 150,000,000
```

---

### Cost Control Tips

| Action | Saving |
|---|---|
| Use **job clusters** (terminate after run) — never all-purpose clusters | ~60% |
| Set **cluster policy** to cap DBUs at 4 nodes max | Prevents runaway spend |
| Keep **Vector Search endpoint stopped** when not demoing | ~₹400/month saved |
| Use `Trigger.AvailableNow` instead of continuous streaming | Pay only per run |
| Run OPTIMIZE + VACUUM weekly, not daily | Reduces cluster hours |
| Enable **auto-terminate** on all-purpose clusters after 30 min idle | Eliminates idle cost |

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
