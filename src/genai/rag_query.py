# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — RAG Query (Vector Search + FMAPI)
# MAGIC
# MAGIC Implements a Retrieval-Augmented Generation (RAG) pipeline:
# MAGIC
# MAGIC 1. **Retrieve**: semantic search over `diagnosis_dim` Vector Search index
# MAGIC 2. **Augment**: inject retrieved clinical context into prompt
# MAGIC 3. **Generate**: call FMAPI (Llama 3.1 70B) with augmented prompt
# MAGIC
# MAGIC Use cases demonstrated:
# MAGIC - Clinical code lookup: "What are the ICD-10 codes for COPD complications?"
# MAGIC - Care gap identification: "Which chronic conditions are most costly in our data?"
# MAGIC - Denial reason analysis: "Why might claims for J44.1 be denied?"

# COMMAND ----------

import requests
import json
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F

CATALOG      = spark.conf.get("pipeline.catalog", "clinicalflow_dev")
SILVER       = f"{CATALOG}.silver"
GOLD         = f"{CATALOG}.gold"
WORKSPACE    = spark.conf.get("spark.databricks.workspaceUrl", "")
VS_ENDPOINT  = "clinicalflow-vs-endpoint"
VS_INDEX     = f"{SILVER}.diagnosis_dim_vs_index"
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"
MAX_TOKENS   = 500

try:
    TOKEN = dbutils.secrets.get(scope="clinicalflow", key="databricks-pat")
except Exception:
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

LLM_URL = f"https://{WORKSPACE}/serving-endpoints/{LLM_ENDPOINT}/invocations"
HEADERS  = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md ## RAG Pipeline Functions

# COMMAND ----------

def retrieve_context(query: str, num_results: int = 5, filters: dict = None) -> list[dict]:
    """
    Step 1 — Retrieve: semantic search over diagnosis_dim vector index.
    Returns a list of matching documents with icd10_code, description, ai_description.
    """
    vsc   = VectorSearchClient(disable_notice=True)
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

    kwargs = dict(
        query_text=query,
        columns=["icd10_code", "description", "ai_description", "category", "chronic_flag"],
        num_results=num_results,
    )
    if filters:
        kwargs["filters"] = filters

    results = index.similarity_search(**kwargs)
    docs    = []
    for row in results.get("result", {}).get("data_array", []):
        docs.append({
            "icd10_code":      row[0],
            "description":     row[1],
            "ai_description":  row[2],
            "category":        row[3],
            "chronic_flag":    row[4],
            "similarity_score": row[-1],
        })
    return docs


def build_rag_prompt(user_question: str, context_docs: list[dict], claim_stats: dict = None) -> str:
    """
    Step 2 — Augment: build a prompt with retrieved clinical context
    and optional claims statistics from Gold tables.
    """
    context_block = "\n".join([
        f"- [{d['icd10_code']}] {d['description']}: {d.get('ai_description', 'N/A')}"
        for d in context_docs
    ])

    stats_block = ""
    if claim_stats:
        stats_block = f"\n\nClaims data from the ClinicalFlow lakehouse:\n{json.dumps(claim_stats, indent=2)}"

    return f"""You are a clinical data analyst assistant for a healthcare organization.
Use the following retrieved ICD-10 code context to answer the question accurately.
If the context does not contain enough information, say so clearly.

RETRIEVED CLINICAL CONTEXT:
{context_block}
{stats_block}

QUESTION: {user_question}

Provide a concise, accurate answer based on the context above. Where relevant, cite specific ICD-10 codes."""


def generate_answer(prompt: str) -> str:
    """
    Step 3 — Generate: call FMAPI with the augmented prompt.
    """
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
        "temperature": 0.2,
    }
    resp = requests.post(LLM_URL, headers=HEADERS, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def rag_query(question: str, num_context: int = 5, filters: dict = None,
              include_claim_stats: bool = False) -> dict:
    """
    Full RAG pipeline: retrieve → augment → generate.
    Returns a dict with question, context documents, and answer.
    """
    # Step 1: Retrieve
    docs = retrieve_context(question, num_results=num_context, filters=filters)

    # Optional: pull claim stats from Gold layer for data-grounded answers
    claim_stats = None
    if include_claim_stats and docs:
        codes = [d["icd10_code"] for d in docs]
        codes_str = ", ".join(f"'{c}'" for c in codes)
        try:
            stats_df = spark.sql(f"""
                SELECT diagnosis_code,
                       SUM(claim_count) AS total_claims,
                       ROUND(SUM(total_amount) / 1e6, 2) AS total_amount_M,
                       ROUND(AVG(avg_amount), 0) AS avg_claim_amount
                FROM {GOLD}.diagnosis_trends
                WHERE diagnosis_code IN ({codes_str})
                GROUP BY diagnosis_code
                ORDER BY total_claims DESC
            """)
            claim_stats = {r["diagnosis_code"]: {
                "total_claims": r["total_claims"],
                "total_amount_M": r["total_amount_M"],
                "avg_claim_amount": r["avg_claim_amount"],
            } for r in stats_df.collect()}
        except Exception:
            pass

    # Step 2: Augment
    prompt = build_rag_prompt(question, docs, claim_stats)

    # Step 3: Generate
    answer = generate_answer(prompt)

    return {
        "question":    question,
        "context_docs": docs,
        "answer":      answer,
    }

# COMMAND ----------

# MAGIC %md ## Demo Query 1 — Clinical Code Lookup

# COMMAND ----------

result1 = rag_query(
    "What are the ICD-10 codes for COPD and its complications?",
    num_context=5,
)

print("=" * 70)
print(f"Q: {result1['question']}\n")
print("Context retrieved:")
for doc in result1["context_docs"]:
    print(f"  [{doc['icd10_code']}] {doc['description']} (score: {doc['similarity_score']:.3f})")
print(f"\nA: {result1['answer']}")

# COMMAND ----------

# MAGIC %md ## Demo Query 2 — Data-Grounded Answer with Claim Stats

# COMMAND ----------

result2 = rag_query(
    "Which chronic respiratory conditions generate the highest claim costs?",
    num_context=5,
    filters={"chronic_flag": True},
    include_claim_stats=True,
)

print("=" * 70)
print(f"Q: {result2['question']}\n")
print("Context retrieved:")
for doc in result2["context_docs"]:
    print(f"  [{doc['icd10_code']}] {doc['description']}")
print(f"\nA: {result2['answer']}")

# COMMAND ----------

# MAGIC %md ## Demo Query 3 — Denial Risk Assessment

# COMMAND ----------

result3 = rag_query(
    "Why might claims for type 2 diabetes complications be denied by payers?",
    num_context=4,
    include_claim_stats=True,
)

print("=" * 70)
print(f"Q: {result3['question']}\n")
print(f"A: {result3['answer']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG Architecture Summary
# MAGIC
# MAGIC ```
# MAGIC User Question
# MAGIC      │
# MAGIC      ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Step 1: RETRIEVE                       │
# MAGIC │  VectorSearchClient.similarity_search() │
# MAGIC │  → top-k docs from diagnosis_dim index  │
# MAGIC └──────────────────┬──────────────────────┘
# MAGIC                    │  context docs
# MAGIC                    ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Step 2: AUGMENT                        │
# MAGIC │  build_rag_prompt(question, docs,       │
# MAGIC │                   claim_stats)          │
# MAGIC │  → structured prompt with context       │
# MAGIC └──────────────────┬──────────────────────┘
# MAGIC                    │  augmented prompt
# MAGIC                    ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Step 3: GENERATE                       │
# MAGIC │  FMAPI: Llama 3.1 70B Instruct          │
# MAGIC │  → grounded clinical answer             │
# MAGIC └─────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | Component | Technology |
# MAGIC |---|---|
# MAGIC | Vector Store | Databricks Vector Search (Delta Sync) |
# MAGIC | Embedding Model | `databricks-gte-large-en` |
# MAGIC | LLM | `databricks-meta-llama-3-1-70b-instruct` |
# MAGIC | Context Source | `diagnosis_dim.ai_description` |
# MAGIC | Grounding Data | `gold.diagnosis_trends` claim statistics |
# MAGIC | Auth | Databricks Secret Scope (`clinicalflow`) |
