# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Delta Time Travel & Change Data Feed
# MAGIC
# MAGIC Demonstrates:
# MAGIC - Delta time travel: `VERSION AS OF` and `TIMESTAMP AS OF`
# MAGIC - `DESCRIBE HISTORY` to inspect transaction log
# MAGIC - Change Data Feed (CDF): `table_changes()` to see row-level changes
# MAGIC - Restoring a table to a previous version
# MAGIC - Row count auditing before/after MERGE operations

# COMMAND ----------

CATALOG = "clinicalflow_dev"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md ## 1 — Delta Transaction History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full transaction log for claims_fact
# MAGIC DESCRIBE HISTORY clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full transaction log for patient_dim (SCD Type 2 — frequent writes)
# MAGIC DESCRIBE HISTORY clinicalflow_dev.silver.patient_dim;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Time Travel: VERSION AS OF

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row count at version 0 (initial load)
# MAGIC SELECT COUNT(*) AS row_count_v0
# MAGIC FROM clinicalflow_dev.silver.claims_fact VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current row count
# MAGIC SELECT COUNT(*) AS row_count_current
# MAGIC FROM clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# Read a specific version in PySpark
v0_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table(f"{SILVER}.claims_fact")

current_df = spark.table(f"{SILVER}.claims_fact")

print(f"Version 0 row count  : {v0_df.count():,}")
print(f"Current row count    : {current_df.count():,}")
print(f"Net new rows         : {current_df.count() - v0_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 3 — Time Travel: TIMESTAMP AS OF

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read state as of a specific timestamp (adjust to match your run)
# MAGIC SELECT COUNT(*) AS claims_at_timestamp
# MAGIC FROM clinicalflow_dev.silver.claims_fact
# MAGIC TIMESTAMP AS OF '2024-01-01 00:00:00';

# COMMAND ----------

# PySpark equivalent
from pyspark.sql import functions as F

ts_df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01 00:00:00") \
    .table(f"{SILVER}.claims_fact")

print(f"Row count at 2024-01-01: {ts_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Change Data Feed: What Changed Between Versions?
# MAGIC
# MAGIC CDF records every row-level operation: `insert`, `update_preimage`,
# MAGIC `update_postimage`, `delete`. Enabled via `delta.enableChangeDataFeed = true`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Changes since version 1 (after initial load)
# MAGIC SELECT _change_type, COUNT(*) AS row_count
# MAGIC FROM table_changes('clinicalflow_dev.silver.claims_fact', 1)
# MAGIC GROUP BY _change_type
# MAGIC ORDER BY _change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full CDF output: see the actual changed rows
# MAGIC SELECT _change_type, _commit_version, _commit_timestamp,
# MAGIC        claim_id, claim_status, claim_amount
# MAGIC FROM table_changes('clinicalflow_dev.silver.claims_fact', 1)
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# CDF in PySpark — between two versions
cdf_df = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(f"{SILVER}.claims_fact")
)

cdf_df.groupBy("_change_type").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Patient SCD Type 2 History Audit
# MAGIC
# MAGIC Since `patient_dim` is SCD Type 2, we can use `__START_AT / __END_AT`
# MAGIC to see how a patient's record evolved over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full history for a single patient (all address/insurance changes)
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     address,
# MAGIC     insurance_plan,
# MAGIC     __START_AT,
# MAGIC     __END_AT,
# MAGIC     __IS_CURRENT
# MAGIC FROM clinicalflow_dev.silver.patient_dim
# MAGIC WHERE patient_id = 'PAT00000001'
# MAGIC ORDER BY __START_AT;

# COMMAND ----------

# MAGIC %md ## 6 — Restore a Table to a Previous Version

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE reverts the table to an earlier version (adds a new transaction log entry)
# MAGIC -- WARNING: This modifies the current state of the table.
# MAGIC -- Only run this in a dev/sandbox environment for demo purposes.
# MAGIC --
# MAGIC -- RESTORE TABLE clinicalflow_dev.silver.claims_fact TO VERSION AS OF 0;
# MAGIC
# MAGIC -- After restore, the table is back to version 0 rows,
# MAGIC -- but the history is preserved — you can always RESTORE forward again.
# MAGIC SELECT 'RESTORE is commented out to protect production data' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Delta Log Inspection (Advanced)
# MAGIC
# MAGIC Delta stores all changes as JSON files in `_delta_log/`.
# MAGIC We can read them directly for deep inspection.

# COMMAND ----------

STORAGE_PATH = "abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/"

# List the Delta transaction log files
log_files = dbutils.fs.ls(f"{STORAGE_PATH}silver/claims_fact/_delta_log/")
for f in log_files[:10]:
    print(f.name, f.size)

# COMMAND ----------

# Read a specific commit JSON to see what changed
import json

# Read version 0 commit file
try:
    commit_0 = spark.read.text(f"{STORAGE_PATH}silver/claims_fact/_delta_log/00000000000000000000.json")
    for row in commit_0.take(5):
        print(json.dumps(json.loads(row.value), indent=2)[:500])
        print("---")
except Exception as e:
    print(f"Log read error (expected if path differs): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Delta Time Travel Capabilities
# MAGIC
# MAGIC | Feature | Syntax | Use Case |
# MAGIC |---|---|---|
# MAGIC | **Version travel** | `VERSION AS OF N` | Audit state at specific operation |
# MAGIC | **Timestamp travel** | `TIMESTAMP AS OF 'ts'` | Regulatory snapshot at point in time |
# MAGIC | **History** | `DESCRIBE HISTORY table` | Inspect all operations + metadata |
# MAGIC | **CDF** | `table_changes('table', startVersion)` | Row-level change feed for downstream |
# MAGIC | **Restore** | `RESTORE TABLE ... TO VERSION AS OF N` | Roll back bad operation |
# MAGIC | **Vacuum** | `VACUUM table RETAIN N HOURS` | Clean up old files (removes travel history) |
# MAGIC
# MAGIC > ⚠️ **VACUUM** removes old Delta files. After vacuuming, you can only time travel
# MAGIC > to versions whose files still exist. Default retention is 7 days.
