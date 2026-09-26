# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — AI Query Demo (ai_query + FMAPI)
# MAGIC
# MAGIC Demonstrates:
# MAGIC - `ai_query()` SQL function — call Foundation Model API from SQL
# MAGIC - Batch enrichment of `diagnosis_dim` with AI-generated clinical descriptions
# MAGIC - `ai_classify()` and `ai_summarize()` for ER admission triage
# MAGIC - Structured JSON extraction from FMAPI responses
# MAGIC - Prompt engineering patterns for healthcare data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

CATALOG = "clinicalflow_dev"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — ai_query() Basics
# MAGIC
# MAGIC `ai_query(endpoint, prompt)` sends a prompt to a Databricks Foundation Model
# MAGIC endpoint and returns the model's response as a string.
# MAGIC
# MAGIC Built-in endpoints (no deployment needed):
# MAGIC - `databricks-dbrx-instruct`
# MAGIC - `databricks-meta-llama-3-1-70b-instruct`
# MAGIC - `databricks-mixtral-8x7b-instruct`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simple ai_query() test
# MAGIC SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What does ICD-10 code E11.9 mean? Reply in one sentence.'
# MAGIC ) AS ai_response;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Batch Enrich diagnosis_dim with AI Descriptions
# MAGIC
# MAGIC The `diagnosis_dim` table was seeded in `02_data_generation.py` with
# MAGIC `ai_description = NULL`. This cell fills those nulls using `ai_query()`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview diagnosis_dim rows that need enrichment
# MAGIC SELECT icd10_code, description, ai_description
# MAGIC FROM clinicalflow_dev.silver.diagnosis_dim
# MAGIC WHERE ai_description IS NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enrich a sample of 20 rows with AI descriptions
# MAGIC -- (Full table enrichment is done in fmapi_enrichment.py job)
# MAGIC CREATE OR REPLACE TEMP VIEW diagnosis_enriched AS
# MAGIC SELECT
# MAGIC     icd10_code,
# MAGIC     description,
# MAGIC     category,
# MAGIC     chronic_flag,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-1-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'You are a clinical terminology expert. For ICD-10 code ',
# MAGIC             icd10_code,
# MAGIC             ' (', description, '), provide:',
# MAGIC             '1. A patient-friendly plain-English explanation (1 sentence)',
# MAGIC             '2. Common symptoms (1 sentence)',
# MAGIC             '3. Typical treatment approach (1 sentence)',
# MAGIC             ' Format as JSON: {"plain_english": "...", "symptoms": "...", "treatment": "..."}'
# MAGIC         )
# MAGIC     ) AS ai_clinical_detail
# MAGIC FROM clinicalflow_dev.silver.diagnosis_dim
# MAGIC WHERE ai_description IS NULL
# MAGIC LIMIT 20;
# MAGIC
# MAGIC SELECT * FROM diagnosis_enriched;

# COMMAND ----------

# MAGIC %md ## 3 — Parse AI JSON Response in PySpark

# COMMAND ----------

from pyspark.sql.functions import from_json, col, get_json_object

enriched_df = spark.table("diagnosis_enriched")

# Extract structured fields from AI JSON output
enriched_structured = (
    enriched_df
    .withColumn("plain_english", get_json_object(col("ai_clinical_detail"), "$.plain_english"))
    .withColumn("symptoms",      get_json_object(col("ai_clinical_detail"), "$.symptoms"))
    .withColumn("treatment",     get_json_object(col("ai_clinical_detail"), "$.treatment"))
)

enriched_structured.select(
    "icd10_code", "description", "plain_english", "symptoms", "treatment"
).show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — ai_classify(): ER Triage Priority Classification
# MAGIC
# MAGIC Classify ER admissions into triage priority levels based on diagnosis + severity.
# MAGIC In production, this would run as a streaming job on `er_admissions_silver`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify sample ER admissions by clinical urgency
# MAGIC SELECT
# MAGIC     admission_id,
# MAGIC     diagnosis_code,
# MAGIC     severity,
# MAGIC     los_hours,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-1-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Triage classification task. Patient has ICD-10 diagnosis code: ',
# MAGIC             diagnosis_code,
# MAGIC             ', clinical severity score: ', severity, '/5',
# MAGIC             ', current length of stay: ', ROUND(los_hours, 1), ' hours.',
# MAGIC             ' Classify triage priority as exactly one of: IMMEDIATE, URGENT, LESS_URGENT, NON_URGENT.',
# MAGIC             ' Reply with JSON: {"priority": "<LEVEL>", "rationale": "<one sentence>"}'
# MAGIC         )
# MAGIC     ) AS triage_response
# MAGIC FROM clinicalflow_dev.silver.er_admissions_silver
# MAGIC WHERE severity >= 3
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md ## 5 — ai_summarize(): Claim Pattern Narrative

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate a natural-language summary of a patient's claim history
# MAGIC WITH patient_claims AS (
# MAGIC     SELECT
# MAGIC         patient_id,
# MAGIC         COLLECT_LIST(
# MAGIC             STRUCT(claim_date, diagnosis_code, claim_amount, claim_status)
# MAGIC         ) AS claims
# MAGIC     FROM clinicalflow_dev.silver.claims_fact
# MAGIC     WHERE patient_id = 'PAT00000001'
# MAGIC     GROUP BY patient_id
# MAGIC )
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-1-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Summarize this patient claim history for a care manager in 2-3 sentences. ',
# MAGIC             'Claims: ', TO_JSON(claims)
# MAGIC         )
# MAGIC     ) AS claim_summary
# MAGIC FROM patient_claims;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Batch AI Enrichment via PySpark (mapPartitions)
# MAGIC
# MAGIC For large-scale enrichment (millions of rows), call the FMAPI
# MAGIC via `mapPartitions` to batch requests efficiently.

# COMMAND ----------

import requests, json, os
from pyspark.sql.types import StringType

def enrich_partition(rows):
    """
    Sends rows to FMAPI in batches of 10.
    Each row gets an AI-generated clinical description.
    """
    token   = dbutils.secrets.get(scope="clinicalflow", key="databricks-pat")
    host    = spark.conf.get("spark.databricks.workspaceUrl")
    api_url = f"https://{host}/serving-endpoints/databricks-meta-llama-3-1-70b-instruct/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for row in rows:
        prompt = (
            f"For ICD-10 code {row.icd10_code} ({row.description}), "
            "give a clinical description in one sentence."
        )
        payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 150}
        try:
            resp = requests.post(api_url, headers=headers, json=payload, timeout=30)
            text = resp.json()["choices"][0]["message"]["content"].strip()
        except Exception:
            text = None
        yield (row.icd10_code, text)

# Apply to a small sample (full run is in fmapi_enrichment.py)
sample_df = (
    spark.table(f"{SILVER}.diagnosis_dim")
    .filter(F.col("ai_description").isNull())
    .select("icd10_code", "description")
    .limit(50)
)

enriched_rdd = sample_df.rdd.mapPartitions(enrich_partition)
enriched_schema = spark.createDataFrame(enriched_rdd, ["icd10_code", "ai_description_new"])
enriched_schema.show(5, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Write AI Enrichments Back to diagnosis_dim

# COMMAND ----------

from delta.tables import DeltaTable

diag_dim = DeltaTable.forName(spark, f"{SILVER}.diagnosis_dim")

diag_dim.alias("target").merge(
    enriched_schema.alias("source"),
    "target.icd10_code = source.icd10_code"
).whenMatchedUpdate(set={
    "ai_description": "source.ai_description_new",
}).execute()

print("✅ AI descriptions merged into diagnosis_dim")
spark.sql(f"""
    SELECT icd10_code, description, ai_description
    FROM {SILVER}.diagnosis_dim
    WHERE ai_description IS NOT NULL
    LIMIT 5
""").show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: FMAPI Patterns in ClinicalFlow
# MAGIC
# MAGIC | Pattern | SQL / PySpark | Use Case |
# MAGIC |---|---|---|
# MAGIC | **`ai_query()`** | SQL inline | Ad-hoc enrichment, small tables |
# MAGIC | **`mapPartitions`** | PySpark | Large-scale batch enrichment |
# MAGIC | **JSON extraction** | `get_json_object()` | Parse structured AI output |
# MAGIC | **Delta MERGE** | `whenMatchedUpdate` | Write AI results back idempotently |
# MAGIC | **Secrets** | `dbutils.secrets.get()` | Never hardcode PAT tokens |
# MAGIC
# MAGIC > Full production enrichment pipeline: `src/genai/fmapi_enrichment.py`
# MAGIC > Vector Search + RAG: `src/genai/vector_search_setup.py` + `rag_query.py`
