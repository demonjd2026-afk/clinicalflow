# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Delta Optimization
# MAGIC
# MAGIC Covers:
# MAGIC - `OPTIMIZE` (compaction) — merges small files into larger ones
# MAGIC - `ZORDER BY` — co-locate related data within files (pre-Liquid Clustering)
# MAGIC - Liquid Clustering — the modern replacement for ZORDER
# MAGIC - `VACUUM` — remove old Delta files beyond retention
# MAGIC - Statistics & `ANALYZE TABLE` for query planning
# MAGIC - Auto-optimization settings

# COMMAND ----------

CATALOG = "clinicalflow_dev"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — OPTIMIZE: File Compaction
# MAGIC
# MAGIC **Problem:** Streaming writes and small batch inserts create many small files.
# MAGIC Small files hurt read performance (many file opens, poor parallelism).
# MAGIC
# MAGIC **Solution:** `OPTIMIZE` merges small files into target file size (~1 GB).
# MAGIC This is idempotent and does not change data — only file layout.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file stats before optimize
# MAGIC DESCRIBE DETAIL clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact claims_fact — merges small files into ~1 GB files
# MAGIC OPTIMIZE clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file stats after optimize
# MAGIC DESCRIBE DETAIL clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — ZORDER BY: Data Skipping (Pre-Liquid Clustering)
# MAGIC
# MAGIC **How it works:** ZORDER rearranges data within files so that rows with
# MAGIC similar values for the ZORDER column are co-located. Delta min/max statistics
# MAGIC then allow the engine to skip entire files during queries.
# MAGIC
# MAGIC **Best for:** High-cardinality columns used in WHERE filters.
# MAGIC **Limitation:** Only 1–2 columns work well (Z-ordering degrades with more columns).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE + ZORDER on Silver tables (most common query pattern: by patient_id, claim_date)
# MAGIC OPTIMIZE clinicalflow_dev.silver.claims_fact
# MAGIC ZORDER BY (patient_id, claim_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ZORDER er_admissions_silver by admitted_at (time-series queries)
# MAGIC OPTIMIZE clinicalflow_dev.silver.er_admissions_silver
# MAGIC ZORDER BY (admitted_at, department);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Liquid Clustering (Modern Replacement for ZORDER)
# MAGIC
# MAGIC **Liquid Clustering** (DBR 13.3+) replaces ZORDER with a more flexible approach:
# MAGIC - Multi-column clustering without Z-order degradation
# MAGIC - Incremental: only newly written data is re-clustered (not full table rewrite)
# MAGIC - No partition management: works alongside or replaces Hive partitioning
# MAGIC
# MAGIC Gold tables were created with `CLUSTER BY` in `gold_aggregations.py`.
# MAGIC Use `OPTIMIZE` (without ZORDER) to trigger liquid clustering passes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trigger liquid clustering pass on Gold tables
# MAGIC -- (CLUSTER BY columns were set at table creation with .clusterBy())
# MAGIC OPTIMIZE clinicalflow_dev.gold.monthly_claims_summary;
# MAGIC OPTIMIZE clinicalflow_dev.gold.provider_performance;
# MAGIC OPTIMIZE clinicalflow_dev.gold.patient_risk_scores;
# MAGIC OPTIMIZE clinicalflow_dev.gold.diagnosis_trends;
# MAGIC OPTIMIZE clinicalflow_dev.gold.er_dept_summary;
# MAGIC OPTIMIZE clinicalflow_dev.gold.network_utilization;
# MAGIC OPTIMIZE clinicalflow_dev.gold.payer_denial_analysis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — VACUUM: Remove Old Files
# MAGIC
# MAGIC **Problem:** Every MERGE, INSERT, and OPTIMIZE creates new files.
# MAGIC Old files accumulate and waste storage.
# MAGIC
# MAGIC **Solution:** `VACUUM` removes files no longer referenced by any Delta version
# MAGIC older than the retention threshold (default: 7 days = 168 hours).
# MAGIC
# MAGIC ⚠️ After VACUUM, time travel before the retention period is no longer possible.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check what VACUUM would remove (DRY RUN — safe, no files deleted)
# MAGIC VACUUM clinicalflow_dev.silver.claims_fact RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Actually vacuum (removes files older than 168 hours / 7 days)
# MAGIC -- In dev: shorter retention is fine; in production keep default
# MAGIC VACUUM clinicalflow_dev.silver.claims_fact RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md ## 5 — Auto-Optimization Settings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable auto-optimize and auto-compaction on a table
# MAGIC ALTER TABLE clinicalflow_dev.silver.claims_fact
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify settings
# MAGIC SHOW TBLPROPERTIES clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Statistics & ANALYZE TABLE
# MAGIC
# MAGIC Delta collects min/max stats for data skipping automatically.
# MAGIC `ANALYZE TABLE` computes column-level statistics used by the Spark cost-based optimizer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compute column statistics for claims_fact (helps Spark query planner)
# MAGIC ANALYZE TABLE clinicalflow_dev.silver.claims_fact COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View statistics
# MAGIC DESCRIBE EXTENDED clinicalflow_dev.silver.claims_fact claim_amount;

# COMMAND ----------

# MAGIC %md ## 7 — Query Performance Comparison

# COMMAND ----------

# Before OPTIMIZE + ZORDER: full scan
import time

start = time.time()
result = spark.sql(f"""
    SELECT COUNT(*) FROM {SILVER}.claims_fact
    WHERE patient_id = 'PAT00000001'
""").collect()
pre_opt_ms = (time.time() - start) * 1000
print(f"Query time: {pre_opt_ms:.0f} ms  |  Result: {result[0][0]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Decision Guide
# MAGIC
# MAGIC | Scenario | Recommendation |
# MAGIC |---|---|
# MAGIC | Streaming pipeline writing small batches | `autoOptimize.optimizeWrite = true` |
# MAGIC | Batch job writing large tables | Manual `OPTIMIZE` after write |
# MAGIC | Query filters on 1–2 high-cardinality cols | `OPTIMIZE ... ZORDER BY (col1, col2)` |
# MAGIC | Query filters on 3+ columns | **Liquid Clustering** (`CLUSTER BY`) |
# MAGIC | Storage cost growing | `VACUUM RETAIN 168 HOURS` weekly |
# MAGIC | Query planner making bad join decisions | `ANALYZE TABLE ... COMPUTE STATISTICS` |
# MAGIC | New table on DBR 13.3+ | Always use Liquid Clustering, skip ZORDER |
