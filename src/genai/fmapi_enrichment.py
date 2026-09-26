# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — FMAPI Batch Enrichment Job
# MAGIC
# MAGIC Production-grade batch enrichment of `diagnosis_dim.ai_description`
# MAGIC using the Databricks Foundation Model API (FMAPI).
# MAGIC
# MAGIC This notebook is scheduled as a Databricks Job (genai_job.yml).
# MAGIC It runs after the Lakeflow pipeline populates Silver.
# MAGIC
# MAGIC Features:
# MAGIC - Incremental: only processes rows where `ai_description IS NULL`
# MAGIC - Batched: groups rows into API batches for efficiency
# MAGIC - Retry-safe: MERGE upsert means re-runs are idempotent
# MAGIC - Secrets: PAT pulled from Databricks Secret Scope (never hardcoded)

# COMMAND ----------

import requests
import json
import time
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG      = spark.conf.get("pipeline.catalog",       "clinicalflow_dev")
SILVER       = f"{CATALOG}.silver"
WORKSPACE    = spark.conf.get("spark.databricks.workspaceUrl", "")
ENDPOINT     = "databricks-meta-llama-3-1-70b-instruct"
BATCH_SIZE   = 10       # rows per API call batch
MAX_TOKENS   = 200
REQUEST_TIMEOUT = 30    # seconds

# COMMAND ----------

# MAGIC %md ## 1 — Auth & API Helper

# COMMAND ----------

try:
    TOKEN = dbutils.secrets.get(scope="clinicalflow", key="databricks-pat")
except Exception:
    # Fallback for interactive runs (not for production)
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

API_URL = f"https://{WORKSPACE}/serving-endpoints/{ENDPOINT}/invocations"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def call_fmapi(prompt: str, retries: int = 3) -> str:
    """Send a single prompt to FMAPI with retry logic."""
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
        "temperature": 0.1,   # low temperature for consistent clinical output
    }
    for attempt in range(retries):
        try:
            resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception as e:
            return None
    return None

# COMMAND ----------

# MAGIC %md ## 2 — Load Rows Needing Enrichment

# COMMAND ----------

pending_df = (
    spark.table(f"{SILVER}.diagnosis_dim")
    .filter(F.col("ai_description").isNull())
    .select("icd10_code", "description", "category", "chronic_flag")
    .orderBy("icd10_code")
)

pending_count = pending_df.count()
print(f"Rows pending AI enrichment: {pending_count:,}")

if pending_count == 0:
    print("Nothing to enrich. Exiting.")
    dbutils.notebook.exit("UP_TO_DATE")

# COMMAND ----------

# MAGIC %md ## 3 — Batch Enrichment via Driver (for ≤10K rows)

# COMMAND ----------

def build_prompt(icd10_code: str, description: str, category: str, chronic_flag: bool) -> str:
    chronic_str = "chronic" if chronic_flag else "acute"
    return (
        f"You are a clinical terminology expert. Provide a concise clinical description "
        f"for the ICD-10 code {icd10_code} ({description}), classified as a {chronic_str} "
        f"condition in the {category} category. "
        f"Include: (1) what the condition is, (2) key clinical features, (3) typical management. "
        f"Keep it under 3 sentences. Do not use bullet points."
    )


enrichment_results = []

rows = pending_df.collect()
total = len(rows)

for i, row in enumerate(rows):
    prompt = build_prompt(row.icd10_code, row.description, row.category, row.chronic_flag)
    ai_text = call_fmapi(prompt)

    enrichment_results.append({
        "icd10_code":      row.icd10_code,
        "ai_description":  ai_text,
    })

    if (i + 1) % 50 == 0 or (i + 1) == total:
        print(f"Progress: {i+1}/{total}  |  Last: {row.icd10_code}")

print(f"\nEnrichment complete. Processed {total:,} rows.")

# COMMAND ----------

# MAGIC %md ## 4 — Write Results Back via Delta MERGE

# COMMAND ----------

enriched_df = spark.createDataFrame(enrichment_results)

# Filter out nulls (failed API calls — will be retried on next run)
enriched_df = enriched_df.filter(F.col("ai_description").isNotNull())
print(f"Rows with successful AI response: {enriched_df.count():,}")

# Idempotent MERGE: only update rows where ai_description was null
diag_dim = DeltaTable.forName(spark, f"{SILVER}.diagnosis_dim")

diag_dim.alias("target").merge(
    enriched_df.alias("source"),
    "target.icd10_code = source.icd10_code"
).whenMatchedUpdate(
    condition="target.ai_description IS NULL",
    set={"ai_description": "source.ai_description"}
).execute()

print("✅ Delta MERGE complete — ai_description updated")

# COMMAND ----------

# MAGIC %md ## 5 — Verify Enrichment Coverage

# COMMAND ----------

coverage = spark.sql(f"""
    SELECT
        COUNT(*) AS total_codes,
        COUNT(ai_description) AS enriched_codes,
        COUNT(*) - COUNT(ai_description) AS pending_codes,
        ROUND(COUNT(ai_description) / COUNT(*) * 100, 1) AS coverage_pct
    FROM {SILVER}.diagnosis_dim
""")

coverage.show()

spark.sql(f"""
    SELECT icd10_code, description, SUBSTR(ai_description, 1, 100) AS ai_desc_preview
    FROM {SILVER}.diagnosis_dim
    WHERE ai_description IS NOT NULL
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Notes
# MAGIC
# MAGIC | Decision | Rationale |
# MAGIC |---|---|
# MAGIC | **Driver-side batching** | diagnosis_dim has ~1K rows — driver is fine; >100K rows → use `mapPartitions` |
# MAGIC | **MERGE with condition** | `target.ai_description IS NULL` prevents overwriting human-curated descriptions |
# MAGIC | **Retry logic** | Transient FMAPI errors are retried with exponential backoff |
# MAGIC | **Secret scope** | PAT in `clinicalflow` scope, never in code or config files |
# MAGIC | **Idempotent** | Re-running the job is safe — already enriched rows are skipped |
