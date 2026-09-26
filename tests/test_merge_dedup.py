"""
tests/test_merge_dedup.py

Unit tests for Delta MERGE deduplication logic (claims_fact).
Uses PySpark local mode — no Delta tables needed.
The MERGE idempotency logic is tested as a pure PySpark transformation.

Run with:
    pytest tests/test_merge_dedup.py -v
"""

import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, TimestampType,
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ClinicalFlow-Dedup-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


CLAIMS_SCHEMA = StructType([
    StructField("claim_id",        StringType(),   False),
    StructField("patient_id",      StringType(),   True),
    StructField("provider_id",     StringType(),   True),
    StructField("diagnosis_code",  StringType(),   True),
    StructField("claim_amount",    DoubleType(),   True),
    StructField("claim_status",    StringType(),   True),
    StructField("claim_date",      DateType(),     True),
    StructField("_updated_ts",     TimestampType(), True),
])


def simulate_merge_upsert(target_df, source_df, key_col: str, update_cols: list[str]):
    """
    Simulates Delta MERGE upsert without Delta:
    - MATCHED: update specified columns from source
    - NOT MATCHED: insert full source row
    Returns the merged DataFrame.
    """
    # Split source into matched (keys in target) and not matched
    matched_keys = target_df.select(key_col)
    matched_source   = source_df.join(matched_keys, key_col, "inner")
    unmatched_source = source_df.join(matched_keys, key_col, "left_anti")

    # For matched: update target rows with source values
    target_not_updated = target_df.join(source_df.select(key_col), key_col, "left_anti")

    # Apply updates: join target with matched source, override update_cols
    target_matched = (
        target_df
        .alias("t")
        .join(matched_source.alias("s"), key_col, "inner")
        .select(
            F.col(f"t.{key_col}"),
            *[
                F.col(f"s.{c}") if c in update_cols else F.col(f"t.{c}")
                for c in [f.name for f in CLAIMS_SCHEMA if f.name != key_col]
            ]
        )
    )

    return target_not_updated.unionByName(target_matched).unionByName(unmatched_source)


class TestMergeDeduplication:

    @pytest.fixture
    def existing_claims(self, spark):
        data = [
            ("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "PENDING",  date(2024, 1, 15), datetime(2024, 1, 15)),
            ("CLM002", "PAT002", "PRV002", "I10",   890.5,  "APPROVED", date(2024, 1, 16), datetime(2024, 1, 16)),
        ]
        return spark.createDataFrame(data, schema=CLAIMS_SCHEMA)

    def test_no_duplicate_on_reinsert(self, spark, existing_claims):
        """Re-inserting the same claim_id must not create duplicates."""
        same_batch = existing_claims   # identical batch
        result = simulate_merge_upsert(
            existing_claims, same_batch, "claim_id",
            ["claim_status", "claim_amount", "_updated_ts"]
        )
        clm001_count = result.filter(F.col("claim_id") == "CLM001").count()
        assert clm001_count == 1, "Re-inserting same claim must not duplicate it"

    def test_status_updated_on_match(self, spark, existing_claims):
        """Matched claim with changed status must have updated status."""
        updates = spark.createDataFrame(
            [("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "APPROVED", date(2024, 1, 15), datetime(2024, 1, 20))],
            schema=CLAIMS_SCHEMA,
        )
        result = simulate_merge_upsert(
            existing_claims, updates, "claim_id",
            ["claim_status", "claim_amount", "_updated_ts"]
        )
        clm001 = result.filter(F.col("claim_id") == "CLM001").collect()
        assert len(clm001) == 1
        assert clm001[0]["claim_status"] == "APPROVED"

    def test_new_claim_inserted(self, spark, existing_claims):
        """New claim_id not in target must be inserted."""
        new_claims = spark.createDataFrame(
            [("CLM999", "PAT003", "PRV003", "J44.1", 3200.0, "APPROVED", date(2024, 1, 17), datetime(2024, 1, 17))],
            schema=CLAIMS_SCHEMA,
        )
        result = simulate_merge_upsert(
            existing_claims, new_claims, "claim_id",
            ["claim_status", "claim_amount", "_updated_ts"]
        )
        assert result.count() == 3
        clm999 = result.filter(F.col("claim_id") == "CLM999").collect()
        assert len(clm999) == 1
        assert clm999[0]["diagnosis_code"] == "J44.1"

    def test_idempotent_on_repeated_merge(self, spark, existing_claims):
        """Running the same batch twice must produce the same result."""
        batch = spark.createDataFrame(
            [("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "APPROVED", date(2024, 1, 15), datetime(2024, 1, 20))],
            schema=CLAIMS_SCHEMA,
        )
        result1 = simulate_merge_upsert(
            existing_claims, batch, "claim_id",
            ["claim_status", "claim_amount", "_updated_ts"]
        )
        result2 = simulate_merge_upsert(
            result1, batch, "claim_id",
            ["claim_status", "claim_amount", "_updated_ts"]
        )
        assert result1.count() == result2.count(), "MERGE must be idempotent"
        assert result2.filter(F.col("claim_id") == "CLM001").count() == 1

    def test_null_claim_id_filtered_upstream(self, spark):
        """claims_fact pipeline filters null claim_ids before MERGE."""
        raw_batch = spark.createDataFrame(
            [
                ("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "APPROVED", date(2024, 1, 15), datetime.utcnow()),
                (None,     "PAT002", "PRV002", "I10",   890.5,  "DENIED",   date(2024, 1, 16), datetime.utcnow()),
            ],
            schema=CLAIMS_SCHEMA,
        )
        # Simulates the filter in claims_fact DLT table
        filtered = raw_batch.filter(F.col("claim_id").isNotNull())
        assert filtered.count() == 1
        assert filtered.collect()[0]["claim_id"] == "CLM001"

    def test_dedup_within_batch_keeps_latest(self, spark):
        """When a batch has duplicate claim_ids, keep only the latest (_updated_ts desc)."""
        from pyspark.sql.window import Window

        batch_with_dups = spark.createDataFrame(
            [
                ("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "PENDING",  date(2024, 1, 15), datetime(2024, 1, 15, 8, 0)),
                ("CLM001", "PAT001", "PRV001", "E11.9", 1250.0, "APPROVED", date(2024, 1, 15), datetime(2024, 1, 15, 9, 0)),
            ],
            schema=CLAIMS_SCHEMA,
        )
        # Deduplication logic from claims_fact DLT table
        deduped = (
            batch_with_dups
            .withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("claim_id").orderBy(F.col("_updated_ts").desc())
                )
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        assert deduped.count() == 1
        assert deduped.collect()[0]["claim_status"] == "APPROVED"
