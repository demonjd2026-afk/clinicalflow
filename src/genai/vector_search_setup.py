# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Vector Search Setup
# MAGIC
# MAGIC Sets up Databricks Vector Search for RAG (Retrieval-Augmented Generation)
# MAGIC over the enriched `diagnosis_dim` table.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create a Vector Search Endpoint
# MAGIC 2. Create a Delta Sync Index on `diagnosis_dim`
# MAGIC    (auto-syncs embeddings when `diagnosis_dim` is updated)
# MAGIC 3. Verify the index with a semantic similarity search
# MAGIC
# MAGIC The index is used by `rag_query.py` to answer clinical questions.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F
import time

CATALOG      = spark.conf.get("pipeline.catalog", "clinicalflow_dev")
SILVER       = f"{CATALOG}.silver"
VS_ENDPOINT  = "clinicalflow-vs-endpoint"
VS_INDEX     = f"{SILVER}.diagnosis_dim_vs_index"
SOURCE_TABLE = f"{SILVER}.diagnosis_dim"
EMBEDDING_MODEL = "databricks-gte-large-en"   # built-in embedding model

# COMMAND ----------

# MAGIC %md ## 1 — Create Vector Search Endpoint

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

# Check if endpoint already exists
existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]

if VS_ENDPOINT not in existing_endpoints:
    print(f"Creating Vector Search endpoint: {VS_ENDPOINT}")
    vsc.create_endpoint(
        name=VS_ENDPOINT,
        endpoint_type="STANDARD",
    )
    # Wait for endpoint to be ready
    print("Waiting for endpoint to be ready...")
    for _ in range(60):
        ep = vsc.get_endpoint(VS_ENDPOINT)
        if ep["endpoint_status"]["state"] == "ONLINE":
            print("✅ Endpoint is ONLINE")
            break
        time.sleep(10)
    else:
        raise RuntimeError("Endpoint did not come online in time")
else:
    print(f"✅ Endpoint '{VS_ENDPOINT}' already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Prepare Source Table
# MAGIC
# MAGIC Vector Search Delta Sync requires:
# MAGIC - A primary key column (`icd10_code`)
# MAGIC - Change Data Feed enabled (`delta.enableChangeDataFeed = true`)
# MAGIC - The text column to embed (`ai_description` or `description`)

# COMMAND ----------

# Enable CDF on diagnosis_dim (required for Delta Sync index)
spark.sql(f"""
    ALTER TABLE {SOURCE_TABLE}
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Verify rows have AI descriptions (needed for embedding)
enriched_count = spark.sql(f"""
    SELECT COUNT(*) AS enriched FROM {SOURCE_TABLE}
    WHERE ai_description IS NOT NULL
""").collect()[0]["enriched"]

print(f"Rows with ai_description: {enriched_count:,}")

if enriched_count < 10:
    print("⚠️  Run fmapi_enrichment.py first to populate ai_description before indexing")

# COMMAND ----------

# MAGIC %md ## 3 — Create Delta Sync Vector Index

# COMMAND ----------

# Check if index already exists
try:
    existing_index = vsc.get_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
    )
    print(f"✅ Index '{VS_INDEX}' already exists. Status: {existing_index['status']['ready']}")
except Exception:
    print(f"Creating Delta Sync index: {VS_INDEX}")
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=SOURCE_TABLE,
        pipeline_type="TRIGGERED",          # manual sync; use CONTINUOUS for real-time
        primary_key="icd10_code",
        embedding_source_column="ai_description",   # column to embed
        embedding_model_endpoint_name=EMBEDDING_MODEL,
    )

    # Wait for index to be ready
    print("Waiting for index to be ready (this takes a few minutes)...")
    for _ in range(120):
        idx = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
        if idx.get("status", {}).get("ready"):
            print("✅ Index is ready")
            break
        time.sleep(15)
    else:
        raise RuntimeError("Index did not become ready in time. Check the Vector Search UI.")

# COMMAND ----------

# MAGIC %md ## 4 — Test Semantic Similarity Search

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

# Search: "heart failure with reduced ejection fraction"
results = index.similarity_search(
    query_text="heart failure with reduced ejection fraction",
    columns=["icd10_code", "description", "ai_description", "category"],
    num_results=5,
)

print("Top 5 results for: 'heart failure with reduced ejection fraction'\n")
for r in results.get("result", {}).get("data_array", []):
    print(f"  {r[0]:<12} {r[1]:<40} score: {r[-1]:.4f}")

# COMMAND ----------

# Cardiology-specific search
results2 = index.similarity_search(
    query_text="type 2 diabetes with complications",
    columns=["icd10_code", "description", "category"],
    filters={"category": "Endocrine, Nutritional and Metabolic Diseases"},   # metadata filter
    num_results=5,
)

print("\nFiltered search — Endocrine conditions matching 'type 2 diabetes with complications':")
for r in results2.get("result", {}).get("data_array", []):
    print(f"  {r[0]:<12} {r[1]:<40} score: {r[-1]:.4f}")

# COMMAND ----------

# MAGIC %md ## 5 — Sync Index After New Enrichments

# COMMAND ----------

# After fmapi_enrichment.py adds more ai_description rows,
# trigger a manual sync to update the embeddings
print("Triggering index sync...")
index.sync()
print("✅ Index sync triggered — check Vector Search UI for progress")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Value |
# MAGIC |---|---|
# MAGIC | **Endpoint** | `clinicalflow-vs-endpoint` (STANDARD tier) |
# MAGIC | **Index type** | Delta Sync (auto-syncs from `diagnosis_dim`) |
# MAGIC | **Embedding model** | `databricks-gte-large-en` (built-in, no deployment) |
# MAGIC | **Primary key** | `icd10_code` |
# MAGIC | **Embedded column** | `ai_description` |
# MAGIC | **Pipeline type** | TRIGGERED (manual sync after enrichment runs) |
# MAGIC | **Filter support** | Metadata filtering on `category`, `chronic_flag` |
# MAGIC
# MAGIC Next: `rag_query.py` — query this index from a chain with LangChain / FMAPI.
