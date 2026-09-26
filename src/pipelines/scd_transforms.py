# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — SCD Transforms + Delta MERGE Upsert
# MAGIC
# MAGIC Standalone notebook demonstrating:
# MAGIC - SCD Type 1 manual MERGE (provider_dim)
# MAGIC - SCD Type 2 manual MERGE with history (patient_dim)
# MAGIC - Delta MERGE upsert for deduplication (claims_fact)
# MAGIC
# MAGIC In the pipeline these are handled by APPLY CHANGES INTO (pipeline_batch.py).
# MAGIC This notebook exists as a standalone demo / reference for interviews.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "clinicalflow_dev"
SILVER  = f"{CATALOG}.silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — SCD Type 1: provider_dim
# MAGIC
# MAGIC **Behaviour:** Latest value overwrites. No history is preserved.
# MAGIC If a provider changes specialty or network status, the old value is gone.
# MAGIC
# MAGIC **MERGE logic:**
# MAGIC - WHEN MATCHED → UPDATE all columns
# MAGIC - WHEN NOT MATCHED → INSERT new row

# COMMAND ----------

# Simulate an incoming provider update batch
provider_updates = spark.createDataFrame([
    ("PRV00000001", "Dr. Alice Smith",  "Cardiology",       "NPI0000000001", "IN_NETWORK",      "TX", "St. Luke's Hospital"),
    ("PRV00000002", "Dr. Bob Johnson",  "Internal Medicine","NPI0000000002", "OUT_OF_NETWORK",   "CA", "Kaiser Permanente"),
    ("PRV99999999", "Dr. New Provider", "Orthopedics",      "NPI9999999999", "IN_NETWORK",       "NY", "NYU Langone"),  # new row
], ["provider_id", "provider_name", "specialty", "npi", "network_status", "state", "hospital_affil"])

provider_updates = provider_updates.withColumn("_updated_ts", F.current_timestamp())

# SCD Type 1 MERGE — overwrite on match, insert on new
provider_dim = DeltaTable.forName(spark, f"{SILVER}.provider_dim")

provider_dim.alias("target").merge(
    provider_updates.alias("source"),
    "target.provider_id = source.provider_id"
).whenMatchedUpdate(set={
    "provider_name":  "source.provider_name",
    "specialty":      "source.specialty",
    "npi":            "source.npi",
    "network_status": "source.network_status",
    "state":          "source.state",
    "hospital_affil": "source.hospital_affil",
    "_updated_ts":    "source._updated_ts",
}).whenNotMatchedInsertAll().execute()

print("SCD Type 1 MERGE complete — provider_dim")
spark.sql(f"SELECT * FROM {SILVER}.provider_dim WHERE provider_id IN ('PRV00000001','PRV99999999')").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — SCD Type 2: patient_dim (manual approach)
# MAGIC
# MAGIC **Behaviour:** Closes the current row (sets `__END_AT`, `__IS_CURRENT = false`),
# MAGIC inserts a new row with updated values and `__IS_CURRENT = true`.
# MAGIC
# MAGIC In the Lakeflow pipeline this is handled automatically by:
# MAGIC `dlt.apply_changes(..., stored_as_scd_type=2)`
# MAGIC which generates `__START_AT`, `__END_AT`, `__IS_CURRENT` columns automatically.
# MAGIC
# MAGIC This cell shows the manual equivalent for reference.

# COMMAND ----------

# Simulate a patient address change
patient_updates = spark.createDataFrame([
    ("PAT00000001", "John",    "Doe",   "1970-05-15", "M", "456 New Address Ave", "Austin", "TX", "78701", "Medicare Advantage", 2),
    ("PAT99999999", "New",     "Member","1985-03-20", "F", "789 Oak Street",       "Dallas", "TX", "75201", "Medicare Part B",    0),
], ["patient_id", "first_name", "last_name", "dob", "gender",
    "address", "city", "state", "zip_code", "insurance_plan", "chronic_conditions"])

patient_updates = (
    patient_updates
    .withColumn("dob", F.col("dob").cast("date"))
    .withColumn("_updated_ts", F.current_timestamp())
    .withColumn("__START_AT",  F.current_timestamp())
    .withColumn("__END_AT",    F.lit(None).cast("timestamp"))
    .withColumn("__IS_CURRENT", F.lit(True))
)

patient_dim = DeltaTable.forName(spark, f"{SILVER}.patient_dim")

# Step 1: Close existing current rows that are changing
patient_dim.alias("target").merge(
    patient_updates.alias("source"),
    "target.patient_id = source.patient_id AND target.__IS_CURRENT = true"
).whenMatchedUpdate(set={
    "__END_AT":     "source.__START_AT",
    "__IS_CURRENT": "false",
}).execute()

# Step 2: Insert new rows for changed + new patients
patient_updates.write.format("delta").mode("append").saveAsTable(f"{SILVER}.patient_dim")

print("SCD Type 2 MERGE complete — patient_dim")
spark.sql(f"""
    SELECT patient_id, address, __START_AT, __END_AT, __IS_CURRENT
    FROM {SILVER}.patient_dim
    WHERE patient_id = 'PAT00000001'
    ORDER BY __START_AT
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Delta MERGE Upsert: claims_fact (Deduplication)
# MAGIC
# MAGIC **Problem:** Autoloader may re-ingest the same file if a job fails and retries.
# MAGIC Without deduplication, the same claim gets inserted multiple times.
# MAGIC
# MAGIC **Solution:** MERGE on `claim_id` — update if exists, insert if new.
# MAGIC This is idempotent — running it twice produces the same result.

# COMMAND ----------

# Simulate a new batch of claims (some overlap with existing data)
new_claims_batch = spark.createDataFrame([
    ("CLM000000001", "PAT00000001", "PRV00000001", "E11.9", "99213", "2024-01-15", 1250.00, "APPROVED",  "PAYER1", "1970-05-15", "6789", "456 New Address Ave"),
    ("CLM000000002", "PAT00000002", "PRV00000002", "I10",   "99214", "2024-01-16", 890.50,  "DENIED",    "PAYER2", "1965-08-22", "1234", "123 Oak St"),
    ("CLM999999999", "PAT00000003", "PRV00000003", "J44.1", "99215", "2024-01-17", 3200.00, "APPROVED",  "PAYER1", "1958-11-30", "5678", "789 Pine Ave"),  # new
], ["claim_id", "patient_id", "provider_id", "diagnosis_code", "procedure_code",
    "claim_date", "claim_amount", "claim_status", "payer_id", "dob", "ssn_last4", "patient_address"])

new_claims_batch = (
    new_claims_batch
    .withColumn("claim_date", F.col("claim_date").cast("date"))
    .withColumn("dob",        F.col("dob").cast("date"))
    .withColumn("_updated_ts", F.current_timestamp())
)

claims_fact = DeltaTable.forName(spark, f"{SILVER}.claims_fact")

# MERGE upsert — idempotent deduplication on claim_id
claims_fact.alias("target").merge(
    new_claims_batch.alias("source"),
    "target.claim_id = source.claim_id"
).whenMatchedUpdate(set={
    # Update mutable fields (status can change from PENDING → APPROVED)
    "claim_status":    "source.claim_status",
    "claim_amount":    "source.claim_amount",
    "patient_address": "source.patient_address",
    "_updated_ts":     "source._updated_ts",
}).whenNotMatchedInsertAll().execute()

print("Delta MERGE upsert complete — claims_fact")

# Verify no duplicates
dup_count = spark.sql(f"""
    SELECT COUNT(*) as total, COUNT(DISTINCT claim_id) as unique_claims
    FROM {SILVER}.claims_fact
""")
dup_count.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Time Travel: Audit Before/After MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Delta history for claims_fact
# MAGIC DESCRIBE HISTORY clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read state before the last MERGE (version - 1)
# MAGIC SELECT COUNT(*) as row_count_before_merge
# MAGIC FROM clinicalflow_dev.silver.claims_fact VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read current state (after MERGE)
# MAGIC SELECT COUNT(*) as row_count_after_merge
# MAGIC FROM clinicalflow_dev.silver.claims_fact;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Change Data Feed: What Changed in Last MERGE?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CDF shows inserts, updates and deletes per operation
# MAGIC SELECT _change_type, COUNT(*) as row_count
# MAGIC FROM table_changes('clinicalflow_dev.silver.claims_fact', 1)
# MAGIC GROUP BY _change_type
# MAGIC ORDER BY _change_type;
