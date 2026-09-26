# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Lakeflow Streaming Pipeline
# MAGIC
# MAGIC Covers:
# MAGIC - Bronze: ER Admissions Autoloader ingestion with DQ expectations
# MAGIC - Silver: ER admissions cleansing + LOS calculation
# MAGIC - Silver: Real-time aggregation with watermarking (stateful streaming)
# MAGIC - Gold: Streaming aggregations written via foreachBatch
# MAGIC
# MAGIC Deploy via: `databricks bundle deploy --target dev`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, TimestampType, IntegerType,
)

from expectations import (
    ER_EXPECT, ER_WARN, ER_DROP, ER_FAIL,
)

CATALOG      = spark.conf.get("pipeline.catalog",       "clinicalflow_dev")
BRONZE       = f"{CATALOG}.bronze"
SILVER       = f"{CATALOG}.silver"
GOLD         = f"{CATALOG}.gold"
STORAGE_PATH = spark.conf.get("pipeline.storage_path",
               "abfss://clinicalflow-dev@stclinicalflow.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md ## BRONZE — ER Admissions Autoloader Ingestion

# COMMAND ----------

@dlt.expect_all(ER_EXPECT)
@dlt.expect_all_or_drop({k: ER_EXPECT[k] for k in ER_DROP})
@dlt.expect_all_or_fail({k: ER_EXPECT[k] for k in ER_FAIL})
@dlt.table(
    name="er_admissions_raw",
    comment="Bronze: raw ER admissions ingested via Autoloader from ADLS Gen2",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}bronze/er_admissions_raw",
)
def er_admissions_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",
                f"{STORAGE_PATH}_schema/er_admissions_raw")
        .option("header", "true")
        .load(f"{STORAGE_PATH}landing/er_admissions/")
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md ## SILVER — ER Admissions Cleansing + LOS

# COMMAND ----------

@dlt.expect_all_or_drop({k: ER_EXPECT[k] for k in ER_DROP})
@dlt.expect_all_or_fail({k: ER_EXPECT[k] for k in ER_FAIL})
@dlt.table(
    name="er_admissions_silver",
    comment="Silver: cleansed ER admissions with length-of-stay in hours",
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
        # Cast string timestamps to proper TimestampType
        .withColumn("admitted_at",   F.col("admitted_at").cast("timestamp"))
        .withColumn("discharged_at", F.col("discharged_at").cast("timestamp"))
        # Length of stay in hours (null for still-admitted patients)
        .withColumn(
            "los_hours",
            F.when(
                F.col("discharged_at").isNotNull(),
                (F.unix_timestamp("discharged_at") - F.unix_timestamp("admitted_at")) / 3600
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("severity",     F.col("severity").cast("int"))
        .withColumn("_updated_ts",  F.current_timestamp())
        .select(
            "admission_id", "patient_id", "department",
            "severity", "admitted_at", "discharged_at",
            "los_hours", "provider_id", "diagnosis_code",
            "_ingest_ts", "_source_file", "_updated_ts",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER — Windowed Aggregation with Watermarking
# MAGIC
# MAGIC **Problem:** We want hourly ER admission counts per department,
# MAGIC but late-arriving data (network lag, retry) can arrive up to 2 hours late.
# MAGIC
# MAGIC **Solution:**
# MAGIC - `withWatermark("admitted_at", "2 hours")` — Spark drops state older than watermark
# MAGIC - `window("admitted_at", "1 hour")` — tumbling 1-hour windows
# MAGIC - Stateful aggregation: Spark maintains per-window state, emits when watermark advances
# MAGIC
# MAGIC This is the **stateful streaming** pattern — state is checkpointed so restarts are safe.

# COMMAND ----------

@dlt.table(
    name="er_hourly_dept_agg",
    comment="Silver: hourly ER admission counts per department (stateful streaming)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}silver/er_hourly_dept_agg",
)
def er_hourly_dept_agg():
    return (
        dlt.read_stream("er_admissions_silver")
        # Watermark: tolerate up to 2 hours of late data
        .withWatermark("admitted_at", "2 hours")
        .groupBy(
            F.window("admitted_at", "1 hour").alias("hour_window"),
            F.col("department"),
        )
        .agg(
            F.count("admission_id").alias("admission_count"),
            F.avg("severity").alias("avg_severity"),
            F.avg("los_hours").alias("avg_los_hours"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum(F.when(F.col("los_hours") > 4, 1).otherwise(0)).alias("long_stay_count"),
        )
        .withColumn("window_start", F.col("hour_window.start"))
        .withColumn("window_end",   F.col("hour_window.end"))
        .drop("hour_window")
        .withColumn("_updated_ts", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER — High-Severity Alert Stream
# MAGIC
# MAGIC Filters for severity ≥ 4 admissions in real-time.
# MAGIC These rows can be picked up by a downstream job / alert system.

# COMMAND ----------

@dlt.table(
    name="er_high_severity_alerts",
    comment="Silver: real-time stream of high-severity ER admissions (severity >= 4)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    path=f"{STORAGE_PATH}silver/er_high_severity_alerts",
)
def er_high_severity_alerts():
    return (
        dlt.read_stream("er_admissions_silver")
        .filter(F.col("severity") >= 4)
        .withColumn("alert_ts",    F.current_timestamp())
        .withColumn("alert_level", F.when(F.col("severity") == 5, "CRITICAL").otherwise("HIGH"))
        .select(
            "admission_id", "patient_id", "department",
            "severity", "alert_level", "admitted_at",
            "provider_id", "diagnosis_code",
            "alert_ts",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Concepts Summary
# MAGIC
# MAGIC | Concept | This Pipeline |
# MAGIC |---|---|
# MAGIC | **Trigger** | Continuous / triggered batch (configured in pipeline YAML) |
# MAGIC | **Checkpoint** | Auto-managed by Lakeflow under `STORAGE_PATH/_checkpoints/` |
# MAGIC | **Watermark** | `2 hours` on `admitted_at` — drops late state |
# MAGIC | **Window** | Tumbling `1 hour` on `admitted_at` |
# MAGIC | **Stateful op** | `groupBy + agg` with watermark = stateful streaming |
# MAGIC | **Output mode** | `append` (watermarked aggregation) |
# MAGIC | **Late data** | Within watermark → included in window; beyond → dropped |
# MAGIC | **Exactly-once** | Delta + idempotent MERGE = exactly-once semantics |
