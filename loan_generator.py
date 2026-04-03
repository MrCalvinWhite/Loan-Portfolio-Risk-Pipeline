"""
loan_generator.py
-----------------
Generates a realistic synthetic loan book for pipeline testing.

Simulates a diversified consumer/commercial lending portfolio
with realistic distributions matching industry data from
FFIEC call reports, HMDA data, and Federal Reserve publications.

PRODUCTION SWAP:
  Replace generate() with a database read:
    df = pd.read_sql("SELECT * FROM originations WHERE status='active'", conn)
  Or from a data lake:
    df = pd.read_parquet("s3://lending-datalake/loans/raw/")
"""

import random
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

log = logging.getLogger("loan_pipeline.generator")

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

LOAN_TYPES = {
    "mortgage":       0.38,
    "personal":       0.25,
    "auto":           0.20,
    "secured_credit": 0.10,
    "heloc":          0.07,
}

STATES = ["TX", "CA", "FL", "NY", "IL", "OH", "GA", "NC", "PA", "AZ",
          "WA", "CO", "NV", "TN", "MI", "MN", "MO", "WI", "OR", "SC"]

LOAN_CONFIG = {
    "mortgage":       {"min": 80_000,   "max": 900_000,  "term_months": [180, 240, 360], "base_rate": 0.068},
    "personal":       {"min": 2_000,    "max": 50_000,   "term_months": [24, 36, 48, 60], "base_rate": 0.115},
    "auto":           {"min": 8_000,    "max": 75_000,   "term_months": [36, 48, 60, 72], "base_rate": 0.079},
    "secured_credit": {"min": 5_000,    "max": 100_000,  "term_months": [60, 84, 120],    "base_rate": 0.095},
    "heloc":          {"min": 15_000,   "max": 250_000,  "term_months": [120, 180, 240],  "base_rate": 0.085},
}


class LoanDataGenerator:
    def __init__(self, n: int = 5000, seed: int = 42):
        self.n = n
        random.seed(seed)

    def generate(self) -> pd.DataFrame:
        rows = []
        loan_types = list(LOAN_TYPES.keys())
        weights    = list(LOAN_TYPES.values())

        for i in range(self.n):
            loan_type = random.choices(loan_types, weights=weights)[0]
            cfg = LOAN_CONFIG[loan_type]

            # Borrower profile
            fico = max(300, min(850, int(random.gauss(690, 75))))
            annual_income = round(max(25_000, random.lognormvariate(11.1, 0.45)), 2)  # ~log-normal income dist, floor at $25K

            # Loan terms
            principal = round(random.uniform(cfg["min"], cfg["max"]), 2)
            term      = random.choice(cfg["term_months"])
            rate_spread = self._fico_spread(fico)
            rate      = round(cfg["base_rate"] + rate_spread + random.uniform(-0.005, 0.005), 5)

            # Collateral (for secured products)
            if loan_type in ("mortgage", "heloc"):
                property_value = round(principal / random.uniform(0.55, 0.95), 2)
                ltv = round(principal / property_value, 4)
            elif loan_type == "auto":
                property_value = round(principal / random.uniform(0.70, 0.99), 2)
                ltv = round(principal / property_value, 4)
            else:
                property_value = None
                ltv = None

            # Monthly payment (amortizing)
            monthly_rate = rate / 12
            if monthly_rate > 0:
                monthly_payment = round(principal * monthly_rate /
                                        (1 - (1 + monthly_rate) ** -term), 2)
            else:
                monthly_payment = round(principal / term, 2)

            # DTI — cap at 0.65 to reflect real-world underwriting limits
            monthly_income = annual_income / 12
            # If payment would cause extreme DTI, scale down the loan
            raw_dti = monthly_payment / monthly_income
            if raw_dti > 0.65:
                principal = round(principal * (0.65 / raw_dti) * 0.95, 2)
                monthly_rate = rate / 12
                if monthly_rate > 0:
                    monthly_payment = round(principal * monthly_rate /
                                            (1 - (1 + monthly_rate) ** -term), 2)
                else:
                    monthly_payment = round(principal / term, 2)
                if loan_type in ("mortgage", "heloc"):
                    property_value = round(principal / random.uniform(0.55, 0.95), 2)
                    ltv = round(principal / property_value, 4)
                elif loan_type == "auto":
                    property_value = round(principal / random.uniform(0.70, 0.99), 2)
                    ltv = round(principal / property_value, 4)
            dti = round(monthly_payment / monthly_income, 4)

            # Origination date
            days_ago = random.randint(30, 365 * 5)
            orig_date = (datetime.today() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

            # Delinquency status (correlated with FICO, DTI)
            default_prob = self._default_probability(fico, dti, loan_type)
            status_roll  = random.random()
            if status_roll < default_prob * 0.3:
                status = "default"
            elif status_roll < default_prob:
                status = random.choice(["30_dpd", "60_dpd", "90_dpd"])
            elif status_roll < default_prob * 1.5:
                status = "watchlist"
            else:
                status = "current"

            rows.append({
                "loan_id":          f"LN{str(i+1).zfill(7)}",
                "loan_type":        loan_type,
                "origination_date": orig_date,
                "principal":        principal,
                "outstanding_balance": round(principal * random.uniform(0.55, 1.0), 2),
                "interest_rate":    rate,
                "term_months":      term,
                "monthly_payment":  monthly_payment,
                "annual_income":    annual_income,
                "fico_score":       fico,
                "dti_ratio":        dti,
                "ltv_ratio":        ltv,
                "property_value":   property_value,
                "state":            random.choice(STATES),
                "loan_status":      status,
                "source_system":    random.choice(["core_banking", "loan_origination_sys", "broker_portal"]),
                "ingested_at":      datetime.utcnow().isoformat(),
            })

        return pd.DataFrame(rows)

    def _fico_spread(self, fico: int) -> float:
        """Interest rate premium based on credit score."""
        if fico >= 780:    return -0.010
        elif fico >= 740:  return  0.000
        elif fico >= 700:  return  0.008
        elif fico >= 660:  return  0.020
        elif fico >= 620:  return  0.040
        elif fico >= 580:  return  0.070
        else:              return  0.120

    def _default_probability(self, fico: int, dti: float, loan_type: str) -> float:
        """Approximate through-the-cycle default probability."""
        base = {"mortgage": 0.03, "personal": 0.08,
                "auto": 0.04, "secured_credit": 0.06, "heloc": 0.04}.get(loan_type, 0.05)
        fico_mult = max(0.1, (750 - fico) / 200)
        dti_mult  = max(0.5, dti / 0.35)
        return min(0.95, base * fico_mult * dti_mult)

    def save_raw(self, df: pd.DataFrame):
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = RAW_DIR / f"loans_raw_{ts}.csv"
        df.to_csv(path, index=False)
        log.info(f"  Raw loan data saved → {path}")
