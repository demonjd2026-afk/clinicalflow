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

## Step 7 — Create Access Connector + Storage Credential + External Locations

> **Why this is needed:** The auto-created workspace credential (`adb_clinicalflow`) is a metastore-only credential — Databricks hard-restricts it to the UC system storage path and it **cannot access your `stclinicalflow` account**. You must create a dedicated Access Connector and storage credential for your ADLS Gen2.

### Step 7a — Create Access Connector for Azure Databricks

1. In Azure Portal → search **"Access Connector for Azure Databricks"** → **Create**
2. Resource group: `rg-clinicalflow`
3. Name: `ac-clinicalflow`
4. Region: **East US 2**
5. Managed Identity: **SystemAssigned** (default)
6. Click **Review + Create** → **Create**

Wait for deployment to complete.

### Step 7b — Assign IAM Role on Storage Account

1. Go to **`stclinicalflow`** (your storage account) → **Access Control (IAM)**
2. Click **+ Add** → **Add role assignment**
3. **Role** tab → search **Storage Blob Data Contributor** → select it → **Next**
4. **Members** tab → Assign access to: **Managed identity** → click **+ Select members**
5. In the side panel:
   - Managed identity dropdown → **Access Connector for Azure Databricks**
   - Select `ac-clinicalflow`
   - Click **Select**
6. Click **Review + assign**

Wait **1–2 minutes** for the role to propagate.

### Step 7c — Create Storage Credential in Databricks

1. In Databricks workspace → **Catalog** (left sidebar) → click **+** → **Create a credential**
2. Credential type: **Azure Managed Identity**
3. Credential name: `clinicalflow_adls`
4. Access Connector ID:
   ```
   /subscriptions/<your-subscription-id>/resourceGroups/rg-clinicalflow/providers/Microsoft.Databricks/accessConnectors/ac-clinicalflow
   ```
   *(Find your subscription ID in Azure Portal → Subscriptions)*
5. Click **Create**

> ⚠️ **Do NOT use `adb_clinicalflow`** for external locations. It is the workspace default credential scoped only to the UC metastore path. Using it will produce `UNAUTHORIZED_ACCESS` errors when creating tables.

### Step 7d — Create External Locations

**Navigation:** Databricks workspace → **Catalog** → **External Data** → **External locations** → **Create external location**

Create all three locations using the table below. For each:
- Storage type: **Azure Data Lake Storage**
- Storage credential: `clinicalflow_adls` ← use the new credential, not `adb_clinicalflow`
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

> If you previously created external locations using `adb_clinicalflow`, drop them and recreate with `clinicalflow_adls`:
> ```sql
> DROP EXTERNAL LOCATION IF EXISTS clinicalflow_dev_loc FORCE;
> DROP EXTERNAL LOCATION IF EXISTS clinicalflow_staging_loc FORCE;
> DROP EXTERNAL LOCATION IF EXISTS clinicalflow_prod_loc FORCE;
> -- then recreate via UI with clinicalflow_adls
> ```

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
| Enable **auto-terminate** on all-purpose clusters after 15 min idle | Eliminates idle cost |

---

## Compute Setup

### vCPU Quota (Required for Interactive Cluster)

New Azure PAYG subscriptions have **0 quota** for most VM families. Request quota before creating a cluster:

1. Azure Portal → **Quotas** → **Compute**
2. Filter region: **East US 2**, search: **Ddsv6**
3. Click the row → **New Quota Request** → enter **8** → Submit
4. Auto-approved within seconds

> **Why Ddsv6?** DSv2 is End of Life. FXmdsv2 nodes are not in the Databricks node list. Ddsv5 requires a support ticket on Basic plan. **Ddsv6 auto-approves instantly.**

### Interactive Cluster (`clinicalflow-dev-cluster`)

| Setting | Value |
|---|---|
| Policy | **Unrestricted** (Personal Compute policy hides Ddsv6 nodes) |
| Runtime | 15.4 LTS |
| Node type | `Standard_D4ads_v6` (4 cores, 16 GB) |
| Mode | Single node |
| Auto-terminate | 15 minutes |

> Use **Unrestricted** policy — Personal Compute restricts the node type list and Ddsv6 does not appear in it.

---

## Infrastructure Summary

| Resource | Name | Notes |
|---|---|---|
| Subscription | `clinicalflow-payg` | Pay As You Go |
| Resource group | `rg-clinicalflow` | East US 2 |
| Storage account | `stclinicalflow` | ADLS Gen2, LRS, HNS enabled |
| Containers | `clinicalflow-dev/staging/prod` | One per environment |
| Databricks workspace | `adb-clinicalflow` | Premium, Hybrid |
| Access Connector | `ac-clinicalflow` | SystemAssigned managed identity |
| Storage credential | `clinicalflow_adls` | Backed by `ac-clinicalflow` |
| External locations | `clinicalflow_dev/staging/prod_loc` | Use `clinicalflow_adls`, not `adb_clinicalflow` |
| UC Catalogs | `clinicalflow_dev/staging/prod` | MANAGED LOCATION required |
| Dev schemas | `bronze`, `silver`, `gold` | Under `clinicalflow_dev` |
| Interactive cluster | `clinicalflow-dev-cluster` | Unrestricted policy, Standard_D4ads_v6 |
| PAT token | `clinicalflow-pat` | 90-day expiry |
