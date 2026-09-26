# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Gold Aggregations
# MAGIC
# MAGIC Produces all 7 Gold tables used by the BI / reporting layer:
# MAGIC
# MAGIC | # | Table | Description |
# MAGIC |---|---|---|
# MAGIC | 1 | `monthly_claims_summary` | Monthly claim volume + amount by payer |
# MAGIC | 2 | `provider_performance` | Approval rate, avg amount, claim volume per provider |
# MAGIC | 3 | `patient_risk_scores` | Chronic condition count + total spend per patient |
# MAGIC | 4 | `diagnosis_trends` | Monthly diagnosis code frequency |
# MAGIC | 5 | `er_dept_summary` | ER department KPIs: admissions, avg LOS, severity |
# MAGIC | 6 | `network_utilization` | In-network vs out-of-network claims breakdown |
# MAGIC | 7 | `payer_denial_analysis` | Denial rates + denied amount by payer |
# MAGIC
# MAGIC All Gold tables use **Liquid Clustering** (`CLUSTER BY`) for optimal query performance.
# MAGIC Run this notebook after pipeline_batch.py has populated Silver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "clinicalflow_dev"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md ## 1 — Monthly Claims Summary

# COMMAND ----------

monthly_claims = (
    spark.table(f"{SILVER}.claims_fact")
    .filter(F.col("claim_id").isNotNull())
    .withColumn("claim_month", F.date_trunc("month", F.col("claim_date")))
    .groupBy("claim_month", "payer_id")
    .agg(
        F.count("claim_id").alias("total_claims"),
        F.sum("claim_amount").alias("total_amount"),
        F.avg("claim_amount").alias("avg_claim_amount"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.countDistinct("provider_id").alias("unique_providers"),
        F.sum(F.when(F.col("claim_status") == "APPROVED",  1).otherwise(0)).alias("approved_count"),
        F.sum(F.when(F.col("claim_status") == "DENIED",    1).otherwise(0)).alias("denied_count"),
        F.sum(F.when(F.col("claim_status") == "PENDING",   1).otherwise(0)).alias("pending_count"),
    )
    .withColumn(
        "approval_rate_pct",
        F.round(F.col("approved_count") / F.col("total_claims") * 100, 2)
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    monthly_claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("claim_month", "payer_id")
    .saveAsTable(f"{GOLD}.monthly_claims_summary")
)

print("✅ monthly_claims_summary written")
spark.sql(f"SELECT claim_month, payer_id, total_claims, approval_rate_pct FROM {GOLD}.monthly_claims_summary ORDER BY claim_month DESC LIMIT 5").show()

# COMMAND ----------

# MAGIC %md ## 2 — Provider Performance

# COMMAND ----------

provider_perf = (
    spark.table(f"{SILVER}.claims_fact").alias("cf")
    .join(
        spark.table(f"{SILVER}.provider_dim").alias("pd"),
        "provider_id",
        "left",
    )
    .groupBy(
        "cf.provider_id",
        F.col("pd.provider_name"),
        F.col("pd.specialty"),
        F.col("pd.network_status"),
        F.col("pd.state"),
    )
    .agg(
        F.count("cf.claim_id").alias("total_claims"),
        F.sum("cf.claim_amount").alias("total_billed"),
        F.avg("cf.claim_amount").alias("avg_claim_amount"),
        F.countDistinct("cf.patient_id").alias("unique_patients"),
        F.sum(F.when(F.col("cf.claim_status") == "APPROVED", 1).otherwise(0)).alias("approved_claims"),
        F.sum(F.when(F.col("cf.claim_status") == "DENIED",   1).otherwise(0)).alias("denied_claims"),
    )
    .withColumn(
        "approval_rate_pct",
        F.round(F.col("approved_claims") / F.col("total_claims") * 100, 2)
    )
    # Rank providers by total billed within each specialty
    .withColumn(
        "specialty_rank",
        F.rank().over(
            Window.partitionBy("specialty")
            .orderBy(F.col("total_billed").desc())
        )
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    provider_perf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("provider_id", "specialty")
    .saveAsTable(f"{GOLD}.provider_performance")
)

print("✅ provider_performance written")
spark.sql(f"SELECT provider_id, specialty, total_claims, approval_rate_pct, specialty_rank FROM {GOLD}.provider_performance ORDER BY total_billed DESC LIMIT 5").show()

# COMMAND ----------

# MAGIC %md ## 3 — Patient Risk Scores

# COMMAND ----------

patient_risk = (
    spark.table(f"{SILVER}.claims_fact").alias("cf")
    .join(
        spark.table(f"{SILVER}.patient_dim")
        .filter(F.col("__IS_CURRENT") == True)  # SCD Type 2: only current rows
        .alias("pd"),
        "patient_id",
        "left",
    )
    .groupBy(
        "cf.patient_id",
        F.col("pd.first_name"),
        F.col("pd.last_name"),
        F.col("pd.insurance_plan"),
        F.col("pd.chronic_conditions"),
    )
    .agg(
        F.count("cf.claim_id").alias("total_claims"),
        F.sum("cf.claim_amount").alias("total_spend"),
        F.avg("cf.claim_amount").alias("avg_claim_amount"),
        F.countDistinct("cf.diagnosis_code").alias("unique_diagnoses"),
        F.countDistinct("cf.provider_id").alias("unique_providers_seen"),
        F.max("cf.claim_date").alias("last_claim_date"),
    )
    # Risk tier: High ≥ $10K spend or ≥ 3 chronic conditions
    .withColumn(
        "risk_tier",
        F.when(
            (F.col("total_spend") >= 10000) | (F.col("chronic_conditions") >= 3),
            "HIGH"
        ).when(
            (F.col("total_spend") >= 5000) | (F.col("chronic_conditions") >= 1),
            "MEDIUM"
        ).otherwise("LOW")
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    patient_risk.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("patient_id", "risk_tier")
    .saveAsTable(f"{GOLD}.patient_risk_scores")
)

print("✅ patient_risk_scores written")
spark.sql(f"""
    SELECT risk_tier, COUNT(*) as patient_count, AVG(total_spend) as avg_spend
    FROM {GOLD}.patient_risk_scores
    GROUP BY risk_tier ORDER BY avg_spend DESC
""").show()

# COMMAND ----------

# MAGIC %md ## 4 — Diagnosis Trends

# COMMAND ----------

diagnosis_trends = (
    spark.table(f"{SILVER}.claims_fact").alias("cf")
    .join(
        spark.table(f"{SILVER}.diagnosis_dim").alias("dd"),
        F.col("cf.diagnosis_code") == F.col("dd.icd10_code"),
        "left",
    )
    .withColumn("claim_month", F.date_trunc("month", F.col("cf.claim_date")))
    .groupBy(
        "claim_month",
        "cf.diagnosis_code",
        F.col("dd.description").alias("diagnosis_description"),
        F.col("dd.category").alias("diagnosis_category"),
        F.col("dd.chronic_flag"),
    )
    .agg(
        F.count("cf.claim_id").alias("claim_count"),
        F.countDistinct("cf.patient_id").alias("patient_count"),
        F.sum("cf.claim_amount").alias("total_amount"),
        F.avg("cf.claim_amount").alias("avg_amount"),
    )
    # Month-over-month change in claim count
    .withColumn(
        "prev_month_count",
        F.lag("claim_count", 1).over(
            Window.partitionBy("cf.diagnosis_code")
            .orderBy("claim_month")
        )
    )
    .withColumn(
        "mom_change_pct",
        F.round(
            (F.col("claim_count") - F.col("prev_month_count")) / F.col("prev_month_count") * 100,
            2
        )
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    diagnosis_trends.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("claim_month", "diagnosis_code")
    .saveAsTable(f"{GOLD}.diagnosis_trends")
)

print("✅ diagnosis_trends written")
spark.sql(f"""
    SELECT diagnosis_code, diagnosis_description, claim_count
    FROM {GOLD}.diagnosis_trends
    ORDER BY claim_count DESC LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 5 — ER Department Summary

# COMMAND ----------

er_dept_summary = (
    spark.table(f"{SILVER}.er_admissions_silver")
    .filter(F.col("admission_id").isNotNull())
    .withColumn("admit_month", F.date_trunc("month", F.col("admitted_at")))
    .groupBy("admit_month", "department")
    .agg(
        F.count("admission_id").alias("total_admissions"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.avg("los_hours").alias("avg_los_hours"),
        F.max("los_hours").alias("max_los_hours"),
        F.avg("severity").alias("avg_severity"),
        F.sum(F.when(F.col("severity") >= 4, 1).otherwise(0)).alias("high_severity_count"),
        F.sum(F.when(F.col("los_hours") > 24, 1).otherwise(0)).alias("long_stay_over_24h"),
        F.sum(F.when(F.col("discharged_at").isNull(), 1).otherwise(0)).alias("still_admitted"),
    )
    .withColumn(
        "high_severity_pct",
        F.round(F.col("high_severity_count") / F.col("total_admissions") * 100, 2)
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    er_dept_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("admit_month", "department")
    .saveAsTable(f"{GOLD}.er_dept_summary")
)

print("✅ er_dept_summary written")
spark.sql(f"""
    SELECT department, SUM(total_admissions) as admissions, ROUND(AVG(avg_los_hours),1) as avg_los
    FROM {GOLD}.er_dept_summary
    GROUP BY department ORDER BY admissions DESC
""").show()

# COMMAND ----------

# MAGIC %md ## 6 — Network Utilization

# COMMAND ----------

network_util = (
    spark.table(f"{SILVER}.claims_fact").alias("cf")
    .join(
        spark.table(f"{SILVER}.provider_dim").alias("pd"),
        "provider_id",
        "left",
    )
    .withColumn("claim_month", F.date_trunc("month", F.col("cf.claim_date")))
    .groupBy("claim_month", F.col("pd.network_status"), F.col("pd.specialty"))
    .agg(
        F.count("cf.claim_id").alias("claim_count"),
        F.sum("cf.claim_amount").alias("total_amount"),
        F.avg("cf.claim_amount").alias("avg_amount"),
        F.countDistinct("cf.patient_id").alias("patient_count"),
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    network_util.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("claim_month", "network_status")
    .saveAsTable(f"{GOLD}.network_utilization")
)

print("✅ network_utilization written")
spark.sql(f"""
    SELECT network_status,
           SUM(claim_count) as claims,
           ROUND(SUM(total_amount)/1e6, 2) as total_amount_M
    FROM {GOLD}.network_utilization
    GROUP BY network_status
""").show()

# COMMAND ----------

# MAGIC %md ## 7 — Payer Denial Analysis

# COMMAND ----------

payer_denial = (
    spark.table(f"{SILVER}.claims_fact")
    .withColumn("claim_month", F.date_trunc("month", F.col("claim_date")))
    .groupBy("claim_month", "payer_id")
    .agg(
        F.count("claim_id").alias("total_claims"),
        F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_count"),
        F.sum(F.when(F.col("claim_status") == "DENIED", F.col("claim_amount")).otherwise(0)).alias("denied_amount"),
        F.sum(F.when(F.col("claim_status") == "APPROVED", F.col("claim_amount")).otherwise(0)).alias("approved_amount"),
        F.countDistinct(
            F.when(F.col("claim_status") == "DENIED", F.col("patient_id"))
        ).alias("patients_with_denials"),
    )
    .withColumn(
        "denial_rate_pct",
        F.round(F.col("denied_count") / F.col("total_claims") * 100, 2)
    )
    .withColumn(
        "denied_amount_pct",
        F.round(
            F.col("denied_amount") / (F.col("denied_amount") + F.col("approved_amount")) * 100,
            2
        )
    )
    # Payer rank by denial rate (highest denial = rank 1)
    .withColumn(
        "denial_rank",
        F.rank().over(
            Window.partitionBy("claim_month")
            .orderBy(F.col("denial_rate_pct").desc())
        )
    )
    .withColumn("_updated_ts", F.current_timestamp())
)

(
    payer_denial.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("claim_month", "payer_id")
    .saveAsTable(f"{GOLD}.payer_denial_analysis")
)

print("✅ payer_denial_analysis written")
spark.sql(f"""
    SELECT payer_id, SUM(total_claims) as claims, ROUND(AVG(denial_rate_pct),2) as avg_denial_rate
    FROM {GOLD}.payer_denial_analysis
    GROUP BY payer_id ORDER BY avg_denial_rate DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All Gold Tables

# COMMAND ----------

gold_tables = [
    "monthly_claims_summary",
    "provider_performance",
    "patient_risk_scores",
    "diagnosis_trends",
    "er_dept_summary",
    "network_utilization",
    "payer_denial_analysis",
]

print(f"\n{'Table':<35} {'Rows':>12}")
print("-" * 50)
for tbl in gold_tables:
    count = spark.table(f"{GOLD}.{tbl}").count()
    icon = "✅" if count > 0 else "⚠️ "
    print(f"{icon} {tbl:<33} {count:>12,}")
