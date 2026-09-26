# ClinicalFlow — Azure Infrastructure Setup Guide

> Complete step-by-step guide to provision the full Azure + Databricks infrastructure for ClinicalFlow.
> Reflects the actual setup experience including real errors encountered and their fixes.

---

## Prerequisites

- A personal Microsoft or Outlook account (Gmail works via Microsoft login)
- A credit/debit card for identity verification (not charged upfront)
- Databricks CLI installed locally (v1.x)
- Git + GitHub Desktop

---

## Step 1 — Create Azure Account

1. Go to [portal.azure.com](https://portal.azure.com)
2. Click **Start free** → sign in with a Microsoft/Outlook account or create one
3. Complete identity verification (credit/debit card required — not charged upfront)
4. Select **Pay As You Go** when prompted for a subscription type
5. Subscription name used: `clinicalflow-payg`

> Azure offers a **$200 free credit for 30 days** on new accounts. The entire dev build fits within this credit.

---

## Step 2 — Set a Cost Alert

> ⚠️ Do this before creating any resources.

1. In Azure Portal → search **Cost Management + Billing** → select your subscription
2. Click **Budgets** → **Add**
3. Budget name: `clinicalflow-budget`
4. Amount: **$40** (≈ ₹3,350), Period: **Monthly**
5. Click **Next** → add alert conditions:
   - 80% of budget → your email
   - 100% of budget → your email
6. Click **Create**

---

## Step 3 — Create a Resource Group

1. Search **Resource groups** → **Create**
2. Subscription: `clinicalflow-payg`
3. Resource group name: `rg-clinicalflow`
4. Region: **East US 2** *(best Databricks Premium availability — do not use Central India)*
5. Click **Review + Create** → **Create**

---

## Step 4 — Create ADLS Gen2 Storage Account

1. Search **Storage accounts** → **Create**
2. Resource group: `rg-clinicalflow`
3. Storage account name: `stclinicalflow` *(globally unique, lowercase, no hyphens)*
4. Region: **East US 2**
5. Performance: **Standard**
6. Redundancy: **LRS** *(cheapest — sufficient for a portfolio project)*
7. Click **Advanced** tab → under **Data Lake Storage Gen2** → enable **Hierarchical namespace** ✅
   - This is what makes it ADLS Gen2 — do not skip this
8. Click **Review + Create** → **Create**

**After creation — create three containers:**

1. Go to the storage account → **Containers** → **+ Container**
2. Create these three containers one by one:

| Container name |
|---|
| `clinicalflow-dev` |
| `clinicalflow-staging` |
| `clinicalflow-prod` |

---

## Step 5 — Create Azure Databricks Workspace

1. Search **Azure Databricks** → **Create**
2. Resource group: `rg-clinicalflow`
3. Workspace name: `adb-clinicalflow`
4. Region: **East US 2**
5. Pricing tier: **Premium (+ Role-based access controls)** ← mandatory for Unity Catalog
6. Workspace type: **Hybrid** ← required; do NOT select Serverless

   | | Serverless | Hybrid |
   |---|---|---|
   | Storage | Databricks-managed | Your own ADLS Gen2 |
   | Custom clusters | ✗ | ✅ |
   | Cluster policies | ✗ | ✅ |
   | External locations | Limited | ✅ Full support |

7. Click **Review + Create** → **Create**

Wait ~2 minutes for deployment. Then click **Launch Workspace**.

---

## Step 6 — Verify Unity Catalog (Auto-Enabled)

> ⚠️ **Do NOT go to [accounts.azuredatabricks.net](https://accounts.azuredatabricks.net)**
>
> Personal Microsoft/Gmail accounts will get the error:
> *"Selected user account does not exist in tenant 'Microsoft Services'"*
>
> This portal is for enterprise Azure AD accounts only. Ignore it completely.

For Azure PAYG with a personal account, **Unity Catalog is automatically enabled** at workspace creation. No manual metastore setup is needed.

**Verify it worked:**

1. Click **Catalog** in the left sidebar of your Databricks workspace
2. You should see a catalog named `adb-clinicalflow` with two schemas: `default` and `information_schema`
3. Unity Catalog is active ✅

---

## Step 7 — Assign IAM Role + Create External Locations

> **Why this is needed:** The auto-created metastore has no default storage root. You must create External Locations and point your catalogs to them explicitly. Before that, the managed identity needs permission to access your storage account.

### Step 7a — Assign IAM Role (Azure Portal)

When Azure creates a Databricks Premium workspace, it auto-creates a managed identity resource called `unity-catalog-access-connector` (type: **Access Connector for Azure Databricks**). Give it storage access:

1. Go to **Azure Portal** → search `stclinicalflow` → open your storage account
2. Left sidebar → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab → search **Storage Blob Data Contributor** → select it → **Next**
5. **Members** tab → Assign access to: **Managed identity** → click **+ Select members**
6. In the side panel:
   - Subscription: `clinicalflow-payg`
   - Managed identity dropdown → select **Access Connector for Azure Databricks (1)**
   - Select `unity-catalog-access-connector` from the list
   - Click **Select**
7. Click **Review + assign**

Wait **1–2 minutes** for the role to propagate before continuing.

### Step 7b — Create External Locations (Databricks Workspace)

**Navigation:** Databricks workspace → **Catalog** (left sidebar) → scroll down → **External Data** → **External locations** → **Create external location**

> ℹ️ **Storage credential is already there.** Azure auto-creates a credential named `adb_clinicalflow (Managed Identity)` — do NOT create one manually. Just select it from the dropdown.

Create all three locations using the table below. For each:
- Storage type: **Azure Data Lake Storage**
- Storage credential: `adb_clinicalflow (Managed Identity)`
- Comment: *(leave blank)*
- Click **Create** (or **Force create** if prompted — the File Events warning is safe to ignore)

| External location name | URL |
|---|---|
| `clinicalflow_dev_loc` | `abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/` |
| `clinicalflow_staging_loc` | `abfss://clinicalflow-staging@stclinicalflow.dfs.core.windows.net/` |
| `clinicalflow_prod_loc` | `abfss://clinicalflow-prod@stclinicalflow.dfs.core.windows.net/` |

**What the validation checks mean:**

| Check | Expected |
|---|---|
| Read / List / Write / Delete | ✅ Success |
| Path Exists | ✅ Success |
| Hierarchical Namespace Enabled | ✅ Success |
| File Events Read | ⚠️ Failed — safe to ignore (EventGrid optimization, not required) |

> ✅ After creating all three, verify under **Catalog → External Data → External locations**.

---

## Step 8 — Create Unity Catalog Catalogs + Schemas

> ⚠️ **Common error:** Running `CREATE CATALOG IF NOT EXISTS clinicalflow_dev;` fails with:
> *"Metastore storage root URL does not exist. Default Storage is enabled in your account."*
>
> **Fix:** Always specify `MANAGED LOCATION` pointing to the ADLS Gen2 container. The external locations from Step 7 must already exist.

Open **SQL Editor** in your Databricks workspace and run all of this:

```sql
-- Create one catalog per environment (MANAGED LOCATION is required)
CREATE CATALOG IF NOT EXISTS clinicalflow_dev
  MANAGED LOCATION 'abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/';

CREATE CATALOG IF NOT EXISTS clinicalflow_staging
  MANAGED LOCATION 'abfss://clinicalflow-staging@stclinicalflow.dfs.core.windows.net/';

CREATE CATALOG IF NOT EXISTS clinicalflow_prod
  MANAGED LOCATION 'abfss://clinicalflow-prod@stclinicalflow.dfs.core.windows.net/';

-- Create Bronze / Silver / Gold schemas inside dev catalog
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.bronze;
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.silver;
CREATE SCHEMA IF NOT EXISTS clinicalflow_dev.gold;
```

Expected result: **OK** for each statement.

> ✅ Go to **Catalog** sidebar → you should now see `clinicalflow_dev`, `clinicalflow_staging`, `clinicalflow_prod` alongside the workspace catalog.

---

## Step 9 — Generate a Personal Access Token (PAT)

1. In Databricks workspace → top-right avatar → **Settings**
2. **Developer** → **Access tokens** → **Generate new token**
3. Name: `clinicalflow-pat`, expiry: **90 days**
4. Copy the token immediately — it is shown only once

---

## Step 10 — Configure Databricks CLI

```bash
# Verify CLI version (v1.x required for DAB support)
databricks --version
# Expected: Databricks CLI v1.x.x

# Configure with workspace URL and PAT
databricks configure --token
# Prompt 1 — Databricks Host: https://adb-<your-workspace-id>.azuredatabricks.net
#             (copy from browser URL when inside the workspace, no trailing slash)
# Prompt 2 — Token: paste your clinicalflow-pat token

# Verify connection
databricks workspace list /
# Expected output: /Repos  /Users  /Shared
```

Credentials are saved globally to `~/.databrickscfg` — run from any directory.

---

## Step 11 — Clone Repo and Deploy with DAB

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

## Step 12 — Bootstrap Synthetic Data

```bash
# Generate 150M synthetic claims in dev
databricks bundle run data_generation_job --target dev

# Verify row counts
# Run in SQL Editor:
# SELECT COUNT(*) FROM clinicalflow_dev.bronze.claims_raw  → 150,000,000
```

---

## Cost Control Tips

| Action | Saving |
|---|---|
| Use **job clusters** (auto-terminate after run) — never all-purpose clusters | ~60% |
| Set **cluster policy** to cap at 4 nodes max | Prevents runaway spend |
| Keep **Vector Search endpoint stopped** when not demoing | ~₹400/month saved |
| Use `Trigger.AvailableNow` instead of continuous streaming | Pay only per run |
| Run OPTIMIZE + VACUUM weekly, not daily | Reduces cluster hours |
| Enable **auto-terminate** on all-purpose clusters after 30 min idle | Eliminates idle cost |

---

## Infrastructure Summary

| Resource | Name | Notes |
|---|---|---|
| Subscription | `clinicalflow-payg` | Pay As You Go |
| Resource group | `rg-clinicalflow` | East US 2 |
| Storage account | `stclinicalflow` | ADLS Gen2, LRS, HNS enabled |
| Containers | `clinicalflow-dev/staging/prod` | One per environment |
| Databricks workspace | `adb-clinicalflow` | Premium, Hybrid |
| Managed identity | `unity-catalog-access-connector` | Auto-created by Azure |
| Storage credential | `adb_clinicalflow (Managed Identity)` | Auto-created by Databricks |
| External locations | `clinicalflow_dev/staging/prod_loc` | One per container |
| UC Catalogs | `clinicalflow_dev/staging/prod` | MANAGED LOCATION required |
| Dev schemas | `bronze`, `silver`, `gold` | Under `clinicalflow_dev` |
| PAT token | `clinicalflow-pat` | 90-day expiry |
