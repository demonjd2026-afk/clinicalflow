"""
tests/test_scd_logic.py

Unit tests for SCD Type 1 and Type 2 MERGE logic.
Uses PySpark local mode (no Databricks cluster needed).

Run with:
    pip install pyspark pytest
    pytest tests/test_scd_logic.py -v
"""

import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, IntegerType, DateType,
)

# ─── Spark fixture ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ClinicalFlow-SCD-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ─── SCD Type 1 helper ───────────────────────────────────────────────────────

def apply_scd1_merge(existing_df, updates_df, key_col: str):
    """
    Simulates SCD Type 1 MERGE in pure PySpark (no Delta).
    - MATCHED: update all columns from source
    - NOT MATCHED: insert new row
    """
    updated_keys   = updates_df.select(key_col)
    not_matched_in_existing = existing_df.join(updated_keys, key_col, "left_anti")
    matched_updated = updates_df   # source wins on match (SCD1)
    return not_matched_in_existing.unionByName(matched_updated)


# ─── SCD Type 2 helper ───────────────────────────────────────────────────────

def apply_scd2_merge(existing_df, updates_df, key_col: str, sequence_col: str):
    """
    Simulates SCD Type 2 MERGE in pure PySpark (no Delta).
    - MATCHED + IS_CURRENT = true: close existing row, insert new row
    - NOT MATCHED: insert new row with IS_CURRENT = true
    """
    now_ts = datetime.utcnow()

    # Close existing current rows that appear in updates
    update_keys = updates_df.select(key_col)
    closed = (
        existing_df
        .join(update_keys, key_col, "inner")
        .filter(F.col("__IS_CURRENT") == True)
        .withColumn("__END_AT",     F.lit(now_ts).cast("timestamp"))
        .withColumn("__IS_CURRENT", F.lit(False))
    )

    # Keep rows that are not being closed (closed or not matching updates)
    unchanged = (
        existing_df
        .join(update_keys, key_col, "left_anti")
        .unionByName(
            existing_df
            .join(update_keys, key_col, "inner")
            .filter(F.col("__IS_CURRENT") == False)
        )
    )

    # New rows from updates
    new_rows = (
        updates_df
        .withColumn("__START_AT",   F.lit(now_ts).cast("timestamp"))
        .withColumn("__END_AT",     F.lit(None).cast("timestamp"))
        .withColumn("__IS_CURRENT", F.lit(True))
    )

    return unchanged.unionByName(closed).unionByName(new_rows)


# ─── Tests: SCD Type 1 ───────────────────────────────────────────────────────

class TestSCDType1:

    @pytest.fixture
    def provider_schema(self):
        return StructType([
            StructField("provider_id",    StringType(), False),
            StructField("provider_name",  StringType(), True),
            StructField("specialty",      StringType(), True),
            StructField("network_status", StringType(), True),
        ])

    @pytest.fixture
    def existing_providers(self, spark, provider_schema):
        data = [
            ("PRV001", "Dr. Alice Smith",  "Cardiology",        "IN_NETWORK"),
            ("PRV002", "Dr. Bob Johnson",  "Internal Medicine", "OUT_OF_NETWORK"),
        ]
        return spark.createDataFrame(data, schema=provider_schema)

    def test_scd1_update_existing_row(self, spark, existing_providers, provider_schema):
        """Matched row: source value should overwrite target."""
        updates = spark.createDataFrame(
            [("PRV001", "Dr. Alice Smith-Chang", "Cardiology", "OUT_OF_NETWORK")],
            schema=provider_schema,
        )
        result = apply_scd1_merge(existing_providers, updates, "provider_id")

        prv001 = result.filter(F.col("provider_id") == "PRV001").collect()
        assert len(prv001) == 1
        assert prv001[0]["provider_name"]  == "Dr. Alice Smith-Chang"
        assert prv001[0]["network_status"] == "OUT_OF_NETWORK"

    def test_scd1_insert_new_row(self, spark, existing_providers, provider_schema):
        """Not-matched row: should be inserted."""
        updates = spark.createDataFrame(
            [("PRV999", "Dr. New Provider", "Orthopedics", "IN_NETWORK")],
            schema=provider_schema,
        )
        result = apply_scd1_merge(existing_providers, updates, "provider_id")

        assert result.count() == 3   # 2 existing + 1 new
        prv999 = result.filter(F.col("provider_id") == "PRV999").collect()
        assert len(prv999) == 1
        assert prv999[0]["specialty"] == "Orthopedics"

    def test_scd1_no_history_preserved(self, spark, existing_providers, provider_schema):
        """SCD1 must not retain the old row — only one row per key."""
        updates = spark.createDataFrame(
            [("PRV001", "Dr. Alice Smith-Chang", "Cardiology", "OUT_OF_NETWORK")],
            schema=provider_schema,
        )
        result = apply_scd1_merge(existing_providers, updates, "provider_id")
        count_prv001 = result.filter(F.col("provider_id") == "PRV001").count()
        assert count_prv001 == 1, "SCD Type 1 must not keep historical rows"


# ─── Tests: SCD Type 2 ───────────────────────────────────────────────────────

class TestSCDType2:

    @pytest.fixture
    def patient_schema(self):
        return StructType([
            StructField("patient_id",   StringType(),   False),
            StructField("address",      StringType(),   True),
            StructField("insurance_plan", StringType(), True),
            StructField("__START_AT",   TimestampType(), True),
            StructField("__END_AT",     TimestampType(), True),
            StructField("__IS_CURRENT", BooleanType(),  True),
        ])

    @pytest.fixture
    def existing_patients(self, spark, patient_schema):
        old_ts = datetime(2023, 1, 1)
        data = [
            ("PAT001", "123 Old St",    "Medicare A",  old_ts, None, True),
            ("PAT002", "456 Oak Ave",   "Medicare B",  old_ts, None, True),
        ]
        return spark.createDataFrame(data, schema=patient_schema)

    def test_scd2_closes_old_row(self, spark, existing_patients, patient_schema):
        """Changed patient: existing IS_CURRENT row must be closed."""
        updates = spark.createDataFrame(
            [("PAT001", "789 New Blvd", "Medicare Advantage", None, None, None)],
            ["patient_id", "address", "insurance_plan", "__START_AT", "__END_AT", "__IS_CURRENT"],
        )
        result = apply_scd2_merge(existing_patients, updates, "patient_id", "_updated_ts")

        old_row = (
            result
            .filter((F.col("patient_id") == "PAT001") & (F.col("address") == "123 Old St"))
            .collect()
        )
        assert len(old_row) == 1
        assert old_row[0]["__IS_CURRENT"] == False
        assert old_row[0]["__END_AT"] is not None

    def test_scd2_inserts_new_current_row(self, spark, existing_patients, patient_schema):
        """Changed patient: a new IS_CURRENT row must be inserted."""
        updates = spark.createDataFrame(
            [("PAT001", "789 New Blvd", "Medicare Advantage", None, None, None)],
            ["patient_id", "address", "insurance_plan", "__START_AT", "__END_AT", "__IS_CURRENT"],
        )
        result = apply_scd2_merge(existing_patients, updates, "patient_id", "_updated_ts")

        new_row = (
            result
            .filter((F.col("patient_id") == "PAT001") & (F.col("__IS_CURRENT") == True))
            .collect()
        )
        assert len(new_row) == 1
        assert new_row[0]["address"] == "789 New Blvd"
        assert new_row[0]["__END_AT"] is None

    def test_scd2_total_rows_after_update(self, spark, existing_patients, patient_schema):
        """2 existing rows + 1 new version = 3 rows total."""
        updates = spark.createDataFrame(
            [("PAT001", "789 New Blvd", "Medicare Advantage", None, None, None)],
            ["patient_id", "address", "insurance_plan", "__START_AT", "__END_AT", "__IS_CURRENT"],
        )
        result = apply_scd2_merge(existing_patients, updates, "patient_id", "_updated_ts")
        assert result.count() == 3

    def test_scd2_unchanged_patient_not_duplicated(self, spark, existing_patients, patient_schema):
        """Patient not in updates should appear exactly once and remain IS_CURRENT."""
        updates = spark.createDataFrame(
            [("PAT001", "789 New Blvd", "Medicare Advantage", None, None, None)],
            ["patient_id", "address", "insurance_plan", "__START_AT", "__END_AT", "__IS_CURRENT"],
        )
        result = apply_scd2_merge(existing_patients, updates, "patient_id", "_updated_ts")

        pat002_rows = result.filter(F.col("patient_id") == "PAT002").collect()
        assert len(pat002_rows) == 1
        assert pat002_rows[0]["__IS_CURRENT"] == True

    def test_scd2_new_patient_inserted(self, spark, existing_patients, patient_schema):
        """Brand-new patient should be inserted with IS_CURRENT = True."""
        updates = spark.createDataFrame(
            [("PAT999", "1 New Member Rd", "Medicaid", None, None, None)],
            ["patient_id", "address", "insurance_plan", "__START_AT", "__END_AT", "__IS_CURRENT"],
        )
        result = apply_scd2_merge(existing_patients, updates, "patient_id", "_updated_ts")

        pat999 = result.filter(F.col("patient_id") == "PAT999").collect()
        assert len(pat999) == 1
        assert pat999[0]["__IS_CURRENT"] == True
        assert pat999[0]["__END_AT"] is None
