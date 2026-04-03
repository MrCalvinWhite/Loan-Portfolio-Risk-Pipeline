"""
loan_quality.py
---------------
Data quality gate for the loan portfolio pipeline.
Validates schema, business rules, regulatory thresholds,
and data completeness before transformation proceeds.
"""

import logging
from datetime import datetime

import pandas as pd

log = logging.getLogger("loan_pipeline.quality")

REQUIRED_COLUMNS = {
    "loan_id", "loan_type", "origination_date", "principal",
    "outstanding_balance", "interest_rate", "term_months",
    "monthly_payment", "annual_income", "fico_score",
    "dti_ratio", "loan_status",
}

VALID_STATUSES  = {"current", "30_dpd", "60_dpd", "90_dpd", "default", "watchlist"}
VALID_TYPES     = {"mortgage", "personal", "auto", "secured_credit", "heloc"}


class LoanQualityChecker:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results = []

    def run_all_checks(self) -> dict:
        self._check_schema()
        self._check_nulls()
        self._check_fico_range()
        self._check_dti_range()
        self._check_positive_amounts()
        self._check_rate_range()
        self._check_valid_categoricals()
        self._check_balance_vs_principal()
        self._check_loan_id_uniqueness()

        passed = all(r["passed"] for r in self.results)
        return {
            "passed": passed,
            "total_checks": len(self.results),
            "passed_checks": sum(1 for r in self.results if r["passed"]),
            "checks": self.results,
        }

    def log_report(self, report: dict):
        status = "PASSED ✓" if report["passed"] else "FAILED ✗"
        log.info(f"  QC Report: {status} — {report['passed_checks']}/{report['total_checks']} checks")
        for c in report["checks"]:
            log.info(f"    [{'✓' if c['passed'] else '✗'}] {c['name']}: {c['message']}")

    def _check_schema(self):
        missing = REQUIRED_COLUMNS - set(self.df.columns)
        self.results.append({
            "name": "schema_completeness",
            "passed": not missing,
            "message": "All required columns present" if not missing else f"Missing: {missing}",
        })

    def _check_nulls(self):
        null_counts = self.df[list(REQUIRED_COLUMNS & set(self.df.columns))].isnull().sum()
        total = null_counts.sum()
        self.results.append({
            "name": "null_values",
            "passed": total == 0,
            "message": "No nulls in required columns" if total == 0
                       else f"{total} nulls found: {null_counts[null_counts > 0].to_dict()}",
        })

    def _check_fico_range(self):
        bad = self.df[(self.df["fico_score"] < 300) | (self.df["fico_score"] > 850)]
        self.results.append({
            "name": "fico_range",
            "passed": len(bad) == 0,
            "message": f"All FICO scores in valid range [300–850]" if not len(bad)
                       else f"{len(bad)} records with invalid FICO scores",
        })

    def _check_dti_range(self):
        bad = self.df[self.df["dti_ratio"] > 1.5]
        self.results.append({
            "name": "dti_range",
            "passed": len(bad) == 0,
            "message": "No extreme DTI outliers (>150%)" if not len(bad)
                       else f"{len(bad)} records with DTI > 150%",
        })

    def _check_positive_amounts(self):
        bad = self.df[(self.df["principal"] <= 0) | (self.df["monthly_payment"] <= 0)]
        self.results.append({
            "name": "positive_amounts",
            "passed": len(bad) == 0,
            "message": "All principal and payment amounts positive" if not len(bad)
                       else f"{len(bad)} records with non-positive amounts",
        })

    def _check_rate_range(self):
        bad = self.df[(self.df["interest_rate"] <= 0) | (self.df["interest_rate"] > 0.40)]
        self.results.append({
            "name": "interest_rate_range",
            "passed": len(bad) == 0,
            "message": "All rates in valid range (0%–40%)" if not len(bad)
                       else f"{len(bad)} records with out-of-range rates",
        })

    def _check_valid_categoricals(self):
        bad_status = set(self.df["loan_status"].unique()) - VALID_STATUSES
        bad_type   = set(self.df["loan_type"].unique()) - VALID_TYPES
        passed = not bad_status and not bad_type
        self.results.append({
            "name": "valid_categoricals",
            "passed": passed,
            "message": "All loan_status and loan_type values valid" if passed
                       else f"Invalid statuses: {bad_status} | Invalid types: {bad_type}",
        })

    def _check_balance_vs_principal(self):
        bad = self.df[self.df["outstanding_balance"] > self.df["principal"] * 1.02]
        self.results.append({
            "name": "balance_vs_principal",
            "passed": len(bad) == 0,
            "message": "Outstanding balances ≤ principal (within 2% tolerance)" if not len(bad)
                       else f"{len(bad)} records with balance exceeding principal",
        })

    def _check_loan_id_uniqueness(self):
        dupes = self.df["loan_id"].duplicated().sum()
        self.results.append({
            "name": "loan_id_uniqueness",
            "passed": dupes == 0,
            "message": "All loan IDs unique" if not dupes
                       else f"{dupes} duplicate loan IDs found",
        })
