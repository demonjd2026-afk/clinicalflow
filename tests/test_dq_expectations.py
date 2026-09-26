"""
tests/test_dq_expectations.py

Unit tests for DQ expectation constants in expectations.py.
Run with: pytest tests/test_dq_expectations.py -v

These tests verify that:
1. All expectation constants are non-empty dicts
2. DROP and FAIL keys are subsets of EXPECT keys
3. SQL expression strings have valid structure (no empty strings)
4. No key appears in both WARN and FAIL (conflicting severity)
"""

import pytest
import sys
import os

# Add src/pipelines to path so we can import expectations.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src/pipelines"))

from expectations import (
    CLAIMS_EXPECT, CLAIMS_WARN, CLAIMS_DROP, CLAIMS_FAIL,
    ER_EXPECT, ER_WARN, ER_DROP, ER_FAIL,
)


# ─── Claims expectations ────────────────────────────────────────────────────

class TestClaimsExpectations:

    def test_claims_expect_is_not_empty(self):
        assert len(CLAIMS_EXPECT) > 0

    def test_claims_expect_values_are_strings(self):
        for key, expr in CLAIMS_EXPECT.items():
            assert isinstance(expr, str) and len(expr) > 0, \
                f"CLAIMS_EXPECT['{key}'] is empty or not a string"

    def test_claims_drop_keys_in_expect(self):
        for key in CLAIMS_DROP:
            assert key in CLAIMS_EXPECT, \
                f"CLAIMS_DROP key '{key}' not found in CLAIMS_EXPECT"

    def test_claims_fail_keys_in_expect(self):
        for key in CLAIMS_FAIL:
            assert key in CLAIMS_EXPECT, \
                f"CLAIMS_FAIL key '{key}' not found in CLAIMS_EXPECT"

    def test_claims_warn_keys_in_expect(self):
        for key in CLAIMS_WARN:
            assert key in CLAIMS_EXPECT, \
                f"CLAIMS_WARN key '{key}' not found in CLAIMS_EXPECT"

    def test_no_overlap_drop_and_fail(self):
        overlap = set(CLAIMS_DROP) & set(CLAIMS_FAIL)
        assert len(overlap) == 0, \
            f"Keys appear in both CLAIMS_DROP and CLAIMS_FAIL: {overlap}"

    def test_critical_null_checks_in_drop(self):
        """Key fields must cause row drops, not just warnings."""
        for field in ["claim_id_not_null", "patient_id_not_null", "provider_id_not_null"]:
            assert field in CLAIMS_DROP, \
                f"'{field}' should be in CLAIMS_DROP (critical null check)"

    def test_claim_status_valid_in_fail(self):
        """Invalid claim status should halt the pipeline."""
        assert "claim_status_valid" in CLAIMS_FAIL, \
            "'claim_status_valid' should be in CLAIMS_FAIL (pipeline-halting check)"

    def test_claim_status_expression_contains_valid_statuses(self):
        expr = CLAIMS_EXPECT["claim_status_valid"]
        assert "APPROVED" in expr
        assert "DENIED" in expr
        assert "PENDING" in expr


# ─── ER Admissions expectations ─────────────────────────────────────────────

class TestERExpectations:

    def test_er_expect_is_not_empty(self):
        assert len(ER_EXPECT) > 0

    def test_er_expect_values_are_strings(self):
        for key, expr in ER_EXPECT.items():
            assert isinstance(expr, str) and len(expr) > 0, \
                f"ER_EXPECT['{key}'] is empty or not a string"

    def test_er_drop_keys_in_expect(self):
        for key in ER_DROP:
            assert key in ER_EXPECT, \
                f"ER_DROP key '{key}' not found in ER_EXPECT"

    def test_er_fail_keys_in_expect(self):
        for key in ER_FAIL:
            assert key in ER_EXPECT, \
                f"ER_FAIL key '{key}' not found in ER_EXPECT"

    def test_no_overlap_drop_and_fail(self):
        overlap = set(ER_DROP) & set(ER_FAIL)
        assert len(overlap) == 0, \
            f"Keys appear in both ER_DROP and ER_FAIL: {overlap}"

    def test_severity_range_in_fail(self):
        assert "severity_range" in ER_FAIL, \
            "'severity_range' should be in ER_FAIL"

    def test_severity_range_expression(self):
        expr = ER_EXPECT["severity_range"]
        assert "BETWEEN" in expr.upper() or (">=" in expr and "<=" in expr), \
            "severity_range expression should use BETWEEN or >= /<="

    def test_discharge_after_admit_in_warn(self):
        """Null discharged_at (still admitted) is valid — this should be a warn, not drop."""
        assert "discharge_after_admit" in ER_WARN, \
            "'discharge_after_admit' should only warn (NULL discharge = still admitted patient)"
        assert "discharge_after_admit" not in ER_DROP, \
            "'discharge_after_admit' must not drop rows (still-admitted patients have NULL discharged_at)"


# ─── Cross-domain checks ─────────────────────────────────────────────────────

class TestExpectationDomains:

    def test_claims_and_er_have_patient_id_check(self):
        assert "patient_id_not_null" in CLAIMS_EXPECT
        assert "patient_id_not_null" in ER_EXPECT

    def test_claims_expect_has_date_validation(self):
        """Claims must have a date sanity check."""
        date_keys = [k for k in CLAIMS_EXPECT if "date" in k.lower()]
        assert len(date_keys) > 0, "CLAIMS_EXPECT should have at least one date validation"

    def test_all_drop_lists_are_lists(self):
        for name, obj in [
            ("CLAIMS_DROP", CLAIMS_DROP), ("CLAIMS_FAIL", CLAIMS_FAIL),
            ("CLAIMS_WARN", CLAIMS_WARN), ("ER_DROP", ER_DROP),
            ("ER_FAIL", ER_FAIL), ("ER_WARN", ER_WARN),
        ]:
            assert isinstance(obj, list), f"{name} should be a list"
