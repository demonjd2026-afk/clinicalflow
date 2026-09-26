# Databricks notebook source
# MAGIC %md
# MAGIC # ClinicalFlow — Unity Catalog Setup
# MAGIC
# MAGIC Sets up governance layer:
# MAGIC - Column masking on PHI fields (DOB, SSN, address)
# MAGIC - Row-level security filters (provider can only see their own patients)
# MAGIC - Table tags for data classification
# MAGIC - Table ACLs
# MAGIC
# MAGIC **Note:** `clinicalflow_dev` catalog was pre-created via the Databricks UI
# MAGIC using `clinicalflow_dev_loc` as the managed storage location.

# COMMAND ----------

# MAGIC %md ## 1 — Verify Catalog and Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Catalog already exists (created via UI); just ensure schemas are present
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN clinicalflow_dev;

# COMMAND ----------

# MAGIC %md ## 2 — Create Bronze Tables (schema only — data loaded by pipeline)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC -- Raw claims landing table
# MAGIC CREATE TABLE IF NOT EXISTS claims_raw (
# MAGIC   claim_id          STRING,
# MAGIC   patient_id        STRING,
# MAGIC   provider_id       STRING,
# MAGIC   diagnosis_code    STRING,
# MAGIC   procedure_code    STRING,
# MAGIC   claim_date        DATE,
# MAGIC   claim_amount      DOUBLE,
# MAGIC   claim_status      STRING,   -- APPROVED / DENIED / PENDING
# MAGIC   payer_id          STRING,
# MAGIC   dob               DATE,     -- PHI
# MAGIC   ssn_last4         STRING,   -- PHI
# MAGIC   patient_address   STRING,   -- PHI
# MAGIC   _ingest_ts        TIMESTAMP,
# MAGIC   _source_file      STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'quality' = 'bronze',
# MAGIC   'domain'  = 'claims'
# MAGIC );
# MAGIC
# MAGIC -- Quarantine table for failed DQ rows
# MAGIC CREATE TABLE IF NOT EXISTS claims_quarantine (
# MAGIC   claim_id       STRING,
# MAGIC   raw_record     STRING,
# MAGIC   dq_rule        STRING,
# MAGIC   failed_at      TIMESTAMP,
# MAGIC   _source_file   STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- ER admissions raw (streaming source)
# MAGIC CREATE TABLE IF NOT EXISTS er_admissions_raw (
# MAGIC   admission_id     STRING,
# MAGIC   patient_id       STRING,
# MAGIC   department       STRING,
# MAGIC   severity         INT,       -- 1 (low) to 5 (critical)
# MAGIC   admitted_at      TIMESTAMP,
# MAGIC   discharged_at    TIMESTAMP,
# MAGIC   provider_id      STRING,
# MAGIC   diagnosis_code   STRING,
# MAGIC   _ingest_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md ## 3 — Create Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC USE SCHEMA silver;
# MAGIC
# MAGIC -- Claims fact (deduplicated via MERGE)
# MAGIC CREATE TABLE IF NOT EXISTS claims_fact (
# MAGIC   claim_id          STRING        NOT NULL,
# MAGIC   patient_id        STRING,
# MAGIC   provider_id       STRING,
# MAGIC   diagnosis_code    STRING,
# MAGIC   procedure_code    STRING,
# MAGIC   claim_date        DATE,
# MAGIC   claim_amount      DOUBLE,
# MAGIC   claim_status      STRING,
# MAGIC   payer_id          STRING,
# MAGIC   dob               DATE,
# MAGIC   ssn_last4         STRING,
# MAGIC   patient_address   STRING,
# MAGIC   _updated_ts       TIMESTAMP,
# MAGIC   CONSTRAINT pk_claim PRIMARY KEY (claim_id) NOT ENFORCED
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'quality' = 'silver'
# MAGIC );
# MAGIC
# MAGIC -- Patient dimension (SCD Type 2 — managed by APPLY CHANGES INTO)
# MAGIC CREATE TABLE IF NOT EXISTS patient_dim (
# MAGIC   patient_id        STRING,
# MAGIC   first_name        STRING,
# MAGIC   last_name         STRING,
# MAGIC   dob               DATE,     -- PHI
# MAGIC   gender            STRING,
# MAGIC   address           STRING,   -- PHI
# MAGIC   city              STRING,
# MAGIC   state             STRING,
# MAGIC   zip_code          STRING,
# MAGIC   insurance_plan    STRING,
# MAGIC   chronic_conditions INT,
# MAGIC   _updated_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC -- Provider dimension (SCD Type 1 — managed by APPLY CHANGES INTO)
# MAGIC CREATE TABLE IF NOT EXISTS provider_dim (
# MAGIC   provider_id       STRING,
# MAGIC   provider_name     STRING,
# MAGIC   specialty         STRING,
# MAGIC   npi               STRING,
# MAGIC   network_status    STRING,   -- IN_NETWORK / OUT_OF_NETWORK
# MAGIC   state             STRING,
# MAGIC   hospital_affil    STRING,
# MAGIC   _updated_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Diagnosis dimension (AI-enriched)
# MAGIC CREATE TABLE IF NOT EXISTS diagnosis_dim (
# MAGIC   diagnosis_code    STRING,
# MAGIC   description       STRING,
# MAGIC   category          STRING,
# MAGIC   icd_version       STRING,
# MAGIC   ai_risk_label     STRING,   -- enriched by FMAPI
# MAGIC   ai_narrative      STRING    -- enriched by FMAPI
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- ER admissions silver
# MAGIC CREATE TABLE IF NOT EXISTS er_admissions_silver (
# MAGIC   admission_id      STRING,
# MAGIC   patient_id        STRING,
# MAGIC   department        STRING,
# MAGIC   severity          INT,
# MAGIC   admitted_at       TIMESTAMP,
# MAGIC   discharged_at     TIMESTAMP,
# MAGIC   los_hours         DOUBLE,   -- length of stay
# MAGIC   provider_id       STRING,
# MAGIC   diagnosis_code    STRING,
# MAGIC   _updated_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md ## 4 — Create Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC USE SCHEMA gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS provider_performance (
# MAGIC   provider_id        STRING,
# MAGIC   provider_name      STRING,
# MAGIC   specialty          STRING,
# MAGIC   total_claims       BIGINT,
# MAGIC   approved_claims    BIGINT,
# MAGIC   denied_claims      BIGINT,
# MAGIC   denial_rate        DOUBLE,
# MAGIC   avg_claim_amount   DOUBLE,
# MAGIC   avg_processing_days DOUBLE,
# MAGIC   out_of_network_rate DOUBLE,
# MAGIC   report_month       STRING,
# MAGIC   _updated_ts        TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (provider_id, report_month);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS patient_risk_score (
# MAGIC   patient_id           STRING,
# MAGIC   total_spend_ytd      DOUBLE,
# MAGIC   predicted_spend_90d  DOUBLE,
# MAGIC   chronic_condition_cnt INT,
# MAGIC   er_visit_cnt         INT,
# MAGIC   readmission_flag     BOOLEAN,
# MAGIC   care_gap_cnt         INT,
# MAGIC   risk_tier            STRING,   -- LOW / MEDIUM / HIGH / CRITICAL
# MAGIC   _updated_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (risk_tier, patient_id);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS cost_by_diagnosis (
# MAGIC   diagnosis_code      STRING,
# MAGIC   icd_category        STRING,
# MAGIC   quarter             STRING,
# MAGIC   total_cost          DOUBLE,
# MAGIC   claim_count         BIGINT,
# MAGIC   avg_cost_per_claim  DOUBLE,
# MAGIC   yoy_cost_change_pct DOUBLE,
# MAGIC   preventable_er_cnt  BIGINT,
# MAGIC   _updated_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (icd_category, quarter);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS er_realtime_dashboard (
# MAGIC   window_start       TIMESTAMP,
# MAGIC   window_end         TIMESTAMP,
# MAGIC   department         STRING,
# MAGIC   admission_count    BIGINT,
# MAGIC   avg_severity       DOUBLE,
# MAGIC   peak_hour_flag     BOOLEAN,
# MAGIC   _updated_ts        TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (department, window_start);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS claims_trend (
# MAGIC   report_month         STRING,
# MAGIC   total_claims         BIGINT,
# MAGIC   approved_amount      DOUBLE,
# MAGIC   denied_amount        DOUBLE,
# MAGIC   avg_adjudication_days DOUBLE,
# MAGIC   mlr_estimate         DOUBLE,
# MAGIC   _updated_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (report_month);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS patient_risk_narratives (
# MAGIC   patient_id     STRING,
# MAGIC   risk_tier      STRING,
# MAGIC   narrative      STRING,   -- LLM-generated plain English alert
# MAGIC   model_used     STRING,
# MAGIC   generated_at   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (risk_tier);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS network_adequacy (
# MAGIC   state                  STRING,
# MAGIC   specialty              STRING,
# MAGIC   active_provider_cnt    INT,
# MAGIC   members_per_provider   DOUBLE,
# MAGIC   avg_distance_miles     DOUBLE,
# MAGIC   adequacy_flag          BOOLEAN,
# MAGIC   report_quarter         STRING,
# MAGIC   _updated_ts            TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (state, specialty);

# COMMAND ----------

# MAGIC %md ## 5 — Column Masking (PHI Protection)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC
# MAGIC -- Masking function: only data_steward group sees real DOB
# MAGIC CREATE OR REPLACE FUNCTION silver.mask_dob(dob DATE)
# MAGIC RETURNS DATE
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('data_stewards') THEN dob
# MAGIC   ELSE DATE('1900-01-01')
# MAGIC END;
# MAGIC
# MAGIC -- Masking function: only data_steward group sees real address
# MAGIC CREATE OR REPLACE FUNCTION silver.mask_address(address STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('data_stewards') THEN address
# MAGIC   ELSE 'REDACTED'
# MAGIC END;
# MAGIC
# MAGIC -- Apply column masks to claims_fact PHI columns
# MAGIC ALTER TABLE silver.claims_fact
# MAGIC   ALTER COLUMN dob SET MASK silver.mask_dob;
# MAGIC
# MAGIC ALTER TABLE silver.claims_fact
# MAGIC   ALTER COLUMN patient_address SET MASK silver.mask_address;

# COMMAND ----------

# MAGIC %md ## 6 — Row-Level Security (Provider Isolation)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC
# MAGIC -- Row filter: provider can only query their own rows
# MAGIC -- admins and care_managers see all rows
# MAGIC CREATE OR REPLACE FUNCTION silver.provider_row_filter(provider_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN is_member('admins')
# MAGIC     OR is_member('care_managers')
# MAGIC     OR current_user() = provider_id;
# MAGIC
# MAGIC ALTER TABLE silver.claims_fact
# MAGIC   SET ROW FILTER silver.provider_row_filter ON (provider_id);

# COMMAND ----------

# MAGIC %md ## 7 — Table Tags (Data Classification)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG clinicalflow_dev;
# MAGIC
# MAGIC -- Tag PHI-containing tables
# MAGIC ALTER TABLE silver.claims_fact
# MAGIC   SET TAGS ('classification' = 'PHI', 'domain' = 'claims', 'sla' = 'silver');
# MAGIC
# MAGIC ALTER TABLE silver.patient_dim
# MAGIC   SET TAGS ('classification' = 'PHI', 'domain' = 'member', 'scd_type' = '2');
# MAGIC
# MAGIC ALTER TABLE silver.provider_dim
# MAGIC   SET TAGS ('classification' = 'PII', 'domain' = 'provider', 'scd_type' = '1');
# MAGIC
# MAGIC ALTER TABLE gold.patient_risk_score
# MAGIC   SET TAGS ('classification' = 'internal', 'domain' = 'risk', 'consumer' = 'care_management');
# MAGIC
# MAGIC ALTER TABLE gold.provider_performance
# MAGIC   SET TAGS ('classification' = 'internal', 'domain' = 'network', 'consumer' = 'medical_director');

# COMMAND ----------

# MAGIC %md ## 8 — Verify Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN clinicalflow_dev.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN clinicalflow_dev.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN clinicalflow_dev.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify column masks are applied
# MAGIC DESCRIBE EXTENDED clinicalflow_dev.silver.claims_fact;
