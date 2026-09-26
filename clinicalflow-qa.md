# ClinicalFlow — Setup & Implementation Q&A

Questions encountered during setup and implementation. Updated as new questions come up.

---

## Azure Setup

### Q1: Why Hybrid workspace type instead of Serverless?

**Serverless workspace**
- Storage is Databricks-managed (you have no control over it)
- Compute is serverless only — no custom cluster configurations
- No support for custom cluster policies
- Limited Unity Catalog external location support
- Less visibility into cost per job

**Hybrid workspace**
- Storage is your own ADLS Gen2 (`stclinicalflow`) — full control
- Supports custom job clusters with auto-terminate and cluster policies
- Full Unity Catalog external location + storage credential support
- Cluster policies let you cap DBU spend per job
- Better for a production-grade project where you control infra

**Decision:** ClinicalFlow uses ADLS Gen2 as external storage and enforces cost control via cluster policies — Hybrid is the only option that supports both.

---

## Unity Catalog Setup

### Q2: Why did accounts.azuredatabricks.net give an error for my personal account?

**Error seen:**
> "Selected user account does not exist in tenant 'Microsoft Services'"

**Why it happens:**
`accounts.azuredatabricks.net` is the Databricks **Account Console** — it is designed for enterprise Azure Active Directory (work/school) accounts managed by an organisation. Personal Microsoft accounts (Outlook, Gmail via Microsoft login) are not part of any enterprise tenant, so Azure rejects the login.

**What to do instead:**
Nothing — you don't need to go there at all.

For Azure PAYG with a personal account, Unity Catalog is **automatically enabled** when you create a Premium workspace. There is no manual metastore creation step.

**How to verify UC is active:**
1. Open your Databricks workspace
2. Click **Catalog** in the left sidebar
3. You should see a catalog named `adb-clinicalflow` with `default` and `information_schema` schemas inside
4. That confirms Unity Catalog is live ✅

---

### Q3: Why did `CREATE CATALOG` fail with a metastore storage root error?

**Error seen:**
> "Metastore storage root URL does not exist. Default Storage is enabled in your account."

**Why it happens:**
When Unity Catalog is auto-enabled for personal accounts, the metastore is created without a default storage root (no ADLS Gen2 path is configured for it automatically). This means Databricks has no place to store managed table data unless you tell it where explicitly.

The plain `CREATE CATALOG` command assumes a default storage root exists — when it doesn't, the command fails.

**The fix — always specify `MANAGED LOCATION`:**

```sql
-- ❌ This fails without a metastore default storage root:
CREATE CATALOG IF NOT EXISTS clinicalflow_dev;

-- ✅ This works — explicitly tells Databricks where to store the catalog's data:
CREATE CATALOG IF NOT EXISTS clinicalflow_dev
  MANAGED LOCATION 'abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/';
```

**Pre-requisite:** The external location for that container (`clinicalflow_dev_loc`) must already exist and the managed identity (`adb-clinicalflow`) must have **Storage Blob Data Contributor** role on `stclinicalflow` in Azure Portal — otherwise the MANAGED LOCATION path will also fail.

**Correct order:**
1. Assign IAM role (Storage Blob Data Contributor) on storage account → managed identity
2. Create external locations in Databricks (one per container)
3. Run `CREATE CATALOG ... MANAGED LOCATION '...'` for each environment

---

---

## Notebook Format

### Q4: Why use `.py` files instead of `.ipynb` for Databricks notebooks?

**`.py` (Databricks source format) — used in this project**

- Native Databricks notebook format — imports directly into the workspace and renders as a full notebook with markdown and code cells
- Works seamlessly with Databricks Asset Bundles (DAB): `databricks bundle deploy` deploys `.py` notebooks automatically with no extra config
- Clean git diffs — each line is readable Python or a comment; reviewers see real code, not JSON
- Industry standard for production Databricks projects on GitHub
- Uses special markers that Databricks understands:
  - `# COMMAND ----------` → cell separator
  - `# MAGIC %md` → markdown cell
  - `# MAGIC %sql` → SQL cell

**`.ipynb` (Jupyter notebook format)**

- JSON-based format designed for Jupyter/JupyterLab and VS Code local development
- Databricks supports it, but it is not the preferred format for DAB deployments
- Git diffs are noisy — every cell change also modifies metadata, output, and execution count fields in the JSON
- Adds unnecessary overhead for a project that runs entirely in Databricks

**Decision:** `.py` files are cleaner for version control, deploy natively via DAB, and are what enterprise Databricks teams use in production. `.ipynb` is better suited when developing locally outside Databricks.

*More questions will be added here as they come up.*
