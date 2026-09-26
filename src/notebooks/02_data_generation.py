# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Synthetic Data Generation
# MAGIC
# MAGIC Generates 150M+ synthetic Medicare claims using:
# MAGIC - CMS SynPUF base data (~2.3M beneficiaries)
# MAGIC - PySpark row inflation to reach 150M claim rows
# MAGIC - Realistic distributions for diagnosis codes, amounts, providers
# MAGIC
# MAGIC **Run once** to bootstrap the dev environment.
# MAGIC Estimated runtime: ~25–35 min on a 4-node cluster.
# MAGIC
# MAGIC > ✅ All dimension tables use `spark.range()` — no Python loops, no GC pressure.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType, TimestampType
)

spark = SparkSession.builder.getOrCreate()

# Target counts
TARGET_CLAIMS      = 150_000_000
TARGET_PATIENTS    =   6_000_000
TARGET_PROVIDERS   =     450_000
TARGET_ER_EVENTS   =  15_000_000

CATALOG = "clinicalflow_dev"
BRONZE  = f"{CATALOG}.bronze"
SILVER  = f"{CATALOG}.silver"

print(f"Generating {TARGET_CLAIMS:,} claims across {TARGET_PATIENTS:,} patients")

# COMMAND ----------

# MAGIC %md ## 1 — Reference Data

# COMMAND ----------

# ICD-10 diagnosis codes (sample — real list is in data/icd10_codes.csv)
DIAGNOSIS_CODES = [
    ("E11.9",  "Type 2 diabetes mellitus without complications",   "Endocrine"),
    ("I10",    "Essential (primary) hypertension",                 "Circulatory"),
    ("J44.1",  "Chronic obstructive pulmonary disease with exacerbation", "Respiratory"),
    ("M54.5",  "Low back pain",                                    "Musculoskeletal"),
    ("F32.9",  "Major depressive disorder, single episode",        "Mental Health"),
    ("N18.3",  "Chronic kidney disease, stage 3",                  "Genitourinary"),
    ("Z00.00", "Encounter for general adult medical examination",   "Preventive"),
    ("I25.10", "Atherosclerotic heart disease",                    "Circulatory"),
    ("E78.5",  "Hyperlipidemia, unspecified",                      "Endocrine"),
    ("J18.9",  "Pneumonia, unspecified organism",                  "Respiratory"),
    ("S72.001A","Fracture of femur",                               "Musculoskeletal"),
    ("C34.10", "Malignant neoplasm of upper lobe bronchus",        "Neoplasms"),
    ("G30.9",  "Alzheimer's disease, unspecified",                 "Neurological"),
    ("K21.0",  "Gastro-esophageal reflux disease with esophagitis","Digestive"),
    ("Z79.4",  "Long-term (current) use of insulin",              "Endocrine"),
]

PROCEDURE_CODES = [
    "99213", "99214", "99215", "99232", "99283",
    "71046", "93000", "80053", "85025", "36415",
    "27447", "43239", "45378", "70553", "93306",
]

SPECIALTIES = [
    "Internal Medicine", "Family Medicine", "Cardiology",
    "Orthopedics", "Pulmonology", "Nephrology",
    "Psychiatry", "Oncology", "Neurology", "Emergency Medicine",
]

STATES = [
    "TX", "CA", "FL", "NY", "PA", "OH", "IL", "NC", "GA", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
]

CLAIM_STATUSES = ["APPROVED", "DENIED", "PENDING"]
STATUS_WEIGHTS = [0.78, 0.15, 0.07]

INSURANCE_PLANS = [
    "Medicare Part A", "Medicare Part B", "Medicare Advantage",
    "Medicaid", "UHC Choice Plus", "Aetna PPO",
]

# COMMAND ----------

# MAGIC %md ## 2 — Generate Provider Dimension (Spark-native, no Python loop)

# COMMAND ----------

# Uses spark.range() — no Python loop, no GC pressure, fully distributed
providers_df = (
    spark.range(TARGET_PROVIDERS)
    .withColumn("provider_id",    F.concat(F.lit("PRV"), F.lpad(F.col("id"), 8, "0")))
    .withColumn("provider_name",  F.concat(F.lit("Dr. Provider "), F.col("id").cast("string")))
    .withColumn("specialty",      F.element_at(
                                      F.array([F.lit(s) for s in SPECIALTIES]),
                                      F.expr(f"cast(id % {len(SPECIALTIES)} + 1 as int)")))
    .withColumn("npi",            F.concat(F.lit("NPI"), F.lpad(F.col("id"), 10, "0")))
    .withColumn("network_status", F.when(F.expr("id % 100 < 82"), F.lit("IN_NETWORK"))
                                   .otherwise(F.lit("OUT_OF_NETWORK")))
    .withColumn("state",          F.element_at(
                                      F.array([F.lit(s) for s in STATES]),
                                      F.expr(f"cast(id % {len(STATES)} + 1 as int)")))
    .withColumn("hospital_affil", F.concat(F.lit("Hospital System "), F.expr("cast(id % 50 as string)")))
    .withColumn("_updated_ts",    F.current_timestamp())
    .drop("id")
)

providers_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.provider_dim")
print(f"provider_dim: {spark.table(f'{SILVER}.provider_dim').count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3 — Generate Patient Dimension (Spark-native, no Python loop)

# COMMAND ----------

# Uses spark.range() — eliminates the 6M-row Python list that caused GC pressure
# dob: spread across 70 years starting 1930 (25,550 days ≈ 70 years)
patients_df = (
    spark.range(TARGET_PATIENTS)
    .withColumn("patient_id",         F.concat(F.lit("PAT"), F.lpad(F.col("id"), 8, "0")))
    .withColumn("first_name",         F.concat(F.lit("FirstName"), F.col("id").cast("string")))
    .withColumn("last_name",          F.concat(F.lit("LastName"),  F.col("id").cast("string")))
    .withColumn("dob",                F.date_add(F.lit("1930-01-01"), F.expr("cast(id % 25550 as int)")))
    .withColumn("gender",             F.when(F.expr("id % 2 = 0"), F.lit("M")).otherwise(F.lit("F")))
    .withColumn("address",            F.concat(F.expr("cast(id % 9999 as string)"), F.lit(" Main St")))
    .withColumn("city",               F.concat(F.lit("City"), F.expr("cast(id % 500 as string)")))
    .withColumn("state",              F.element_at(
                                          F.array([F.lit(s) for s in STATES]),
                                          F.expr(f"cast(id % {len(STATES)} + 1 as int)")))
    .withColumn("zip_code",           F.lpad(F.expr("cast(10000 + (id % 89999) as string)"), 5, "0"))
    .withColumn("insurance_plan",     F.element_at(
                                          F.array([F.lit(p) for p in INSURANCE_PLANS]),
                                          F.expr(f"cast(id % {len(INSURANCE_PLANS)} + 1 as int)")))
    .withColumn("chronic_conditions", F.expr("cast(id % 9 as int)"))
    .withColumn("_updated_ts",        F.current_timestamp())
    .drop("id")
)

patients_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.patient_dim")
print(f"patient_dim: {spark.table(f'{SILVER}.patient_dim').count():,} rows")

# COMMAND ----------

# MAGIC %md ## 4 — Generate Claims (150M rows via PySpark inflation)

# COMMAND ----------

# Strategy: generate a base dataset of ~5M unique claims,
# then cross-join with a multiplier range to inflate to 150M.
# Each inflated row gets a unique claim_id.

BASE_CLAIMS = 5_000_000
MULTIPLIER  = TARGET_CLAIMS // BASE_CLAIMS   # 30x

diag_codes  = [d[0] for d in DIAGNOSIS_CODES]
proc_codes  = PROCEDURE_CODES

# Base claims dataframe
base_claims_df = (
    spark.range(BASE_CLAIMS)
    .withColumn("patient_id",      F.concat(F.lit("PAT"), F.lpad(F.expr("id % 6000000"), 8, "0")))
    .withColumn("provider_id",     F.concat(F.lit("PRV"), F.lpad(F.expr("id % 450000"),  8, "0")))
    .withColumn("diagnosis_code",  F.element_at(F.array([F.lit(c) for c in diag_codes]),
                                                F.expr(f"cast(id % {len(diag_codes)} + 1 as int)")))
    .withColumn("procedure_code",  F.element_at(F.array([F.lit(c) for c in proc_codes]),
                                                F.expr(f"cast(id % {len(proc_codes)} + 1 as int)")))
    .withColumn("claim_date",      F.date_add(F.lit("2020-01-01"), F.expr("cast(id % 1825 as int)")))
    .withColumn("claim_amount",    F.round(F.abs(F.randn(42)) * 5000 + 200, 2))
    .withColumn("claim_status",    F.when(F.expr("id % 100 < 78"), F.lit("APPROVED"))
                                    .when(F.expr("id % 100 < 93"), F.lit("DENIED"))
                                    .otherwise(F.lit("PENDING")))
    .withColumn("payer_id",        F.concat(F.lit("PAYER"), F.expr("cast(id % 10 as string)")))
    .withColumn("dob",             F.date_add(F.lit("1930-01-01"), F.expr("cast(id % 25550 as int)")))
    .withColumn("ssn_last4",       F.lpad(F.expr("cast(id % 9999 as string)"), 4, "0"))
    .withColumn("patient_address", F.concat(F.expr("cast(id % 9999 as string)"), F.lit(" Main St")))
    .withColumn("_ingest_ts",      F.current_timestamp())
    .withColumn("_source_file",    F.lit("synthetic_generation_v1"))
    .drop("id")
)

# Inflate 30x: cross-join with multiplier range
multiplier_df = spark.range(MULTIPLIER).withColumnRenamed("id", "mult_id")

inflated_claims_df = (
    base_claims_df
    .crossJoin(multiplier_df)
    .withColumn("claim_id", F.concat(
        F.lit("CLM"),
        F.lpad(F.expr("monotonically_increasing_id()"), 15, "0")
    ))
    .drop("mult_id")
)

# Write to bronze in batches (repartition to avoid small files)
(
    inflated_claims_df
    .repartition(200)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BRONZE}.claims_raw")
)

count = spark.table(f"{BRONZE}.claims_raw").count()
print(f"claims_raw: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 5 — Generate ER Admissions Stream Data (15M rows)

# COMMAND ----------

DEPARTMENTS = ["Emergency", "ICU", "Cardiology", "Trauma", "Pediatrics", "Neurology"]

er_df = (
    spark.range(TARGET_ER_EVENTS)
    .withColumn("admission_id",   F.concat(F.lit("ER"), F.lpad(F.col("id"), 10, "0")))
    .withColumn("patient_id",     F.concat(F.lit("PAT"), F.lpad(F.expr("id % 6000000"), 8, "0")))
    .withColumn("department",     F.element_at(F.array([F.lit(d) for d in DEPARTMENTS]),
                                               F.expr(f"cast(id % {len(DEPARTMENTS)} + 1 as int)")))
    .withColumn("severity",       F.expr("cast(id % 5 + 1 as int)"))
    .withColumn("admitted_at",    F.expr("timestamp('2023-01-01') + make_interval(0, 0, 0, 0, cast(id % 8760 as int))"))
    .withColumn("discharged_at",  F.expr("admitted_at + make_interval(0, 0, 0, 0, cast(id % 72 + 1 as int))"))
    .withColumn("provider_id",    F.concat(F.lit("PRV"), F.lpad(F.expr("id % 450000"), 8, "0")))
    .withColumn("diagnosis_code", F.element_at(F.array([F.lit(c) for c in diag_codes]),
                                               F.expr(f"cast(id % {len(diag_codes)} + 1 as int)")))
    .withColumn("_ingest_ts",     F.current_timestamp())
    .drop("id")
)

(
    er_df
    .repartition(100)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BRONZE}.er_admissions_raw")
)

count = spark.table(f"{BRONZE}.er_admissions_raw").count()
print(f"er_admissions_raw: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 6 — Load Diagnosis Dimension

# COMMAND ----------

diag_data = [
    (code, desc, cat, "ICD-10-CM", None, None)
    for code, desc, cat in DIAGNOSIS_CODES
]

diag_schema = StructType([
    StructField("diagnosis_code", StringType()),
    StructField("description",    StringType()),
    StructField("category",       StringType()),
    StructField("icd_version",    StringType()),
    StructField("ai_risk_label",  StringType()),
    StructField("ai_narrative",   StringType()),
])

diag_df = spark.createDataFrame(diag_data, schema=diag_schema)
diag_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.diagnosis_dim")
print(f"diagnosis_dim: {diag_df.count()} rows (AI enrichment runs in genai_job)")

# COMMAND ----------

# MAGIC %md ## 7 — Verify Row Counts

# COMMAND ----------

tables = {
    "bronze.claims_raw":        TARGET_CLAIMS,
    "bronze.er_admissions_raw": TARGET_ER_EVENTS,
    "silver.patient_dim":       TARGET_PATIENTS,
    "silver.provider_dim":      TARGET_PROVIDERS,
    "silver.diagnosis_dim":     len(DIAGNOSIS_CODES),
}

print("\n=== Data Generation Summary ===")
for tbl, expected in tables.items():
    actual = spark.table(f"{CATALOG}.{tbl}").count()
    status = "✅" if actual >= expected * 0.99 else "⚠️"
    print(f"{status}  {tbl:<35} {actual:>15,}  (expected ~{expected:,})")
