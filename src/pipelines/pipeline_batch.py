# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Lakeflow Declarative Pipeline (Batch)
# MAGIC
# MAGIC Covers:
# MAGIC - Bronze: Autoloader (cloudFiles) ingestion with DQ expectations
# MAGIC - Silver: SCD Type 1 (provider_dim) + SCD Type 2 (patient_dim) via APPLY CHANGES INTO
# MAGIC - Silver: claims_fact deduplication via Delta MERGE
# MAGIC - Silver: diagnosis_dim enrichment merge
# MAGIC
# MAGIC This file is the pipeline source for `lakeflow_pipeline.yml`.
# MAGIC Deploy via: `databricks bundle deploy --target dev`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType, IntegerType

from expectations import (
    CLAIMS_EXPECT, CLAIMS_WARN, CLAIMS_DROP, CLAIMS_FAIL,
    ER_EXPECT, ER_WARN, ER_DROP, ER_FAIL,
)

# Pipeline parameters — injected by databricks.yml per environment
CATALOG      = spark.conf.get("pipeline.catalog",       "clinicalflow_dev")
BRONZE       = f"{CATALOG}.bronze"
SILVER       = f"{CATALOG}.silver"
STORAGE_PATH = spark.conf.get("pipeline.storage_path",
               "abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md ## BRONZE — Claims Autoloader Ingestion

# COMMAND ----------

@dlt.expect_all(CLAIMS_EXPECT)
@dlt.expect_all_or_drop({k: CLAIMS_EXPECT[k] for k in CLAIMS_DROP})
@dlt.expect_all_or_fail({k: CLAIMS_EXPECT[k] for k in CLAIMS_FAIL})
@dlt.table(
    name="claims_raw",
    comment="Bronze: raw claims ingested via Autoloader from ADLS Gen2",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}bronze/claims_raw",
)
def claims_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",
                f"{STORAGE_PATH}_schema/claims_raw")
        .option("header", "true")
        .load(f"{STORAGE_PATH}landing/claims/")
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md ## BRONZE — Quarantine (rows dropped by DQ)

# COMMAND ----------

@dlt.table(
    name="claims_quarantine",
    comment="Bronze: claims rows that failed DQ expectations",
    table_properties={"quality": "quarantine"},
    path=f"{STORAGE_PATH}bronze/claims_quarantine",
)
def claims_quarantine():
    # Capture rows that were dropped by expect_or_drop rules
    return (
        dlt.read_stream("claims_raw")
        .filter(
            F.col("claim_id").isNull() |
            F.col("patient_id").isNull() |
            F.col("provider_id").isNull()
        )
        .withColumn("dq_rule",    F.lit("null_key_field"))
        .withColumn("raw_record", F.to_json(F.struct("*")))
        .withColumn("failed_at",  F.current_timestamp())
        .select("claim_id", "raw_record", "dq_rule", "failed_at", "_source_file")
    )

# COMMAND ----------

# MAGIC %md ## SILVER — claims_fact (Delta MERGE deduplication)

# COMMAND ----------

@dlt.table(
    name="claims_fact",
    comment="Silver: deduplicated claims fact table, upserted via MERGE",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}silver/claims_fact",
)
def claims_fact():
    """
    Reads from Bronze claims_raw, deduplicates on claim_id.
    In the Lakeflow pipeline this materialized view is refreshed
    incrementally. Full MERGE upsert logic is in scd_transforms.py
    for the standalone job path.
    """
    return (
        dlt.read("claims_raw")
        .filter(F.col("claim_id").isNotNull())
        .withColumn(
            "row_num",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("claim_id")
                .orderBy(F.col("_ingest_ts").desc())
            ),
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_updated_ts", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## SILVER — provider_dim (SCD Type 1 via APPLY CHANGES INTO)

# COMMAND ----------

# Source feed for APPLY CHANGES INTO (streaming table)
@dlt.table(
    name="provider_updates_feed",
    comment="Staging feed of provider dimension changes",
    temporary=True,
)
def provider_updates_feed():
    """
    In production this would read from a CDC source or a dedicated feed.
    For this project we derive provider updates from claims_raw.
    """
    return (
        dlt.read_stream("claims_raw")
        .select(
            F.col("provider_id"),
            F.lit(None).cast("string").alias("provider_name"),
            F.lit(None).cast("string").alias("specialty"),
            F.lit(None).cast("string").alias("npi"),
            F.lit("IN_NETWORK").alias("network_status"),
            F.lit(None).cast("string").alias("state"),
            F.lit(None).cast("string").alias("hospital_affil"),
            F.col("_ingest_ts").alias("_updated_ts"),
        )
        .filter(F.col("provider_id").isNotNull())
        .dropDuplicates(["provider_id"])
    )

# SCD Type 1 — APPLY CHANGES INTO overwrites existing rows (no history)
dlt.create_streaming_table(
    name="provider_dim",
    comment="Silver: provider dimension — SCD Type 1 (latest value overwrites)",
    table_properties={
        "quality": "silver",
        "scd_type": "1",
    },
    path=f"{STORAGE_PATH}silver/provider_dim",
)

dlt.apply_changes(
    target="provider_dim",
    source="provider_updates_feed",
    keys=["provider_id"],
    sequence_by=F.col("_updated_ts"),
    stored_as_scd_type=1,          # SCD Type 1: overwrite
)

# COMMAND ----------

# MAGIC %md ## SILVER — patient_dim (SCD Type 2 via APPLY CHANGES INTO)

# COMMAND ----------

@dlt.table(
    name="patient_updates_feed",
    comment="Staging feed of patient dimension changes",
    temporary=True,
)
def patient_updates_feed():
    """
    Derives patient updates from claims_raw.
    In production: connect to member eligibility CDC feed.
    """
    return (
        dlt.read_stream("claims_raw")
        .select(
            F.col("patient_id"),
            F.lit(None).cast("string").alias("first_name"),
            F.lit(None).cast("string").alias("last_name"),
            F.col("dob"),
            F.lit(None).cast("string").alias("gender"),
            F.col("patient_address").alias("address"),
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("string").alias("state"),
            F.lit(None).cast("string").alias("zip_code"),
            F.lit(None).cast("string").alias("insurance_plan"),
            F.lit(0).cast("int").alias("chronic_conditions"),
            F.col("_ingest_ts").alias("_updated_ts"),
        )
        .filter(F.col("patient_id").isNotNull())
        .dropDuplicates(["patient_id"])
    )

# SCD Type 2 — APPLY CHANGES INTO keeps full history
# Auto-generates: __START_AT, __END_AT, __IS_CURRENT columns
dlt.create_streaming_table(
    name="patient_dim",
    comment="Silver: patient dimension — SCD Type 2 (full history preserved)",
    table_properties={
        "quality": "silver",
        "scd_type": "2",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}silver/patient_dim",
)

dlt.apply_changes(
    target="patient_dim",
    source="patient_updates_feed",
    keys=["patient_id"],
    sequence_by=F.col("_updated_ts"),
    stored_as_scd_type=2,          # SCD Type 2: preserve history
    track_history_column_list=[    # only track changes to these columns
        "address", "insurance_plan", "chronic_conditions"
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type Comparison
# MAGIC
# MAGIC | | SCD Type 1 | SCD Type 2 |
# MAGIC |---|---|---|
# MAGIC | **Table** | `provider_dim` | `patient_dim` |
# MAGIC | **Behaviour** | Overwrites existing row | Inserts new row, closes old |
# MAGIC | **History** | No history kept | Full history via `__START_AT/__END_AT` |
# MAGIC | **`__IS_CURRENT`** | Not generated | Auto-generated (`true` for latest) |
# MAGIC | **Use case** | Provider name/NPI corrections | Patient address/insurance changes |
# MAGIC | **`stored_as_scd_type`** | `1` | `2` |

# COMMAND ----------

# MAGIC %md ## SILVER — er_admissions_silver

# COMMAND ----------

@dlt.expect_all_or_drop({k: ER_EXPECT[k] for k in ER_DROP})
@dlt.expect_all_or_fail({k: ER_EXPECT[k] for k in ER_FAIL})
@dlt.table(
    name="er_admissions_silver",
    comment="Silver: cleansed ER admissions with length-of-stay calculation",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}silver/er_admissions_silver",
)
def er_admissions_silver():
    return (
        dlt.read_stream("er_admissions_raw")
        .filter(F.col("admission_id").isNotNull())
        .withColumn(
            "los_hours",
            (F.unix_timestamp("discharged_at") - F.unix_timestamp("admitted_at")) / 3600,
        )
        .withColumn("_updated_ts", F.current_timestamp())
        .select(
            "admission_id", "patient_id", "department",
            "severity", "admitted_at", "discharged_at",
            "los_hours", "provider_id", "diagnosis_code", "_updated_ts",
        )
    )
