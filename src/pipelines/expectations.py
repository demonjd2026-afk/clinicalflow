# src/pipelines/expectations.py
# Shared DQ expectation constants for Lakeflow Declarative Pipelines.
# Import this module in pipeline_batch.py and pipeline_streaming.py.

# --------------------------------------------------------------------------- #
#  Claims Raw — Bronze expectations
# --------------------------------------------------------------------------- #

CLAIMS_EXPECT = {
    "claim_id_not_null": "claim_id IS NOT NULL",
    "patient_id_not_null": "patient_id IS NOT NULL",
    "provider_id_not_null": "provider_id IS NOT NULL",
    "claim_amount_positive": "claim_amount > 0",
    "claim_date_valid": "claim_date >= '2015-01-01' AND claim_date <= current_date()",
    "claim_status_valid": "claim_status IN ('APPROVED', 'DENIED', 'PENDING')",
    "diagnosis_code_not_null": "diagnosis_code IS NOT NULL",
}

# expect        → log violation, keep row
# expect_or_drop → drop row (goes to quarantine)
# expect_or_fail → halt pipeline

CLAIMS_WARN   = ["claim_amount_positive", "claim_date_valid"]
CLAIMS_DROP   = ["claim_id_not_null", "patient_id_not_null", "provider_id_not_null"]
CLAIMS_FAIL   = ["claim_status_valid"]

# --------------------------------------------------------------------------- #
#  ER Admissions — Bronze expectations
# --------------------------------------------------------------------------- #

ER_EXPECT = {
    "admission_id_not_null": "admission_id IS NOT NULL",
    "patient_id_not_null":   "patient_id IS NOT NULL",
    "severity_range":        "severity BETWEEN 1 AND 5",
    "admitted_at_not_null":  "admitted_at IS NOT NULL",
    "discharge_after_admit": "discharged_at IS NULL OR discharged_at > admitted_at",
}

ER_WARN = ["discharge_after_admit"]
ER_DROP = ["admission_id_not_null", "patient_id_not_null", "admitted_at_not_null"]
ER_FAIL = ["severity_range"]
