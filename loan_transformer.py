"""
loan_transformer.py
-------------------
Risk scoring and enrichment transformation layer.

Applies:
  - Credit risk tiering (PD bands based on FICO + DTI)
  - LTV risk classification (for secured loans)
  - DTI stress classification (QM rule thresholds)
  - Expected Loss (EL) calculation: EL = PD × LGD × EAD
  - Vintage cohort tagging
  - Delinquency severity scoring
  - Policy exception flagging
  - Portfolio concentration risk flags
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

log = logging.getLogger("loan_pipeline.transformer")

STAGING_DIR = Path("data/staging")
STAGING_DIR.mkdir(parents=True, exist_ok=True)


class LoanTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def transform(self) -> pd.DataFrame:
        df = self._cast_types(self.df)
        log.info("  Computing credit risk tiers...")
        df = self._credit_risk_tier(df)
        log.info("  Calculating LTV risk bands...")
        df = self._ltv_risk_band(df)
        log.info("  Applying DTI stress classification...")
        df = self._dti_classification(df)
        log.info("  Computing Expected Loss (EL = PD × LGD × EAD)...")
        df = self._expected_loss(df)
        log.info("  Tagging vintage cohorts...")
        df = self._vintage_cohort(df)
        log.info("  Scoring delinquency severity...")
        df = self._delinquency_score(df)
        log.info("  Flagging policy exceptions...")
        df = self._policy_exceptions(df)
        log.info("  Computing annualized revenue...")
        df = self._revenue_metrics(df)
        df["pipeline_version"] = "1.0.0"
        df["transformed_at"]   = datetime.utcnow().isoformat()
        return df.reset_index(drop=True)

    def _cast_types(self, df):
        df["origination_date"] = pd.to_datetime(df["origination_date"])
        df["principal"]             = df["principal"].astype(float)
        df["outstanding_balance"]   = df["outstanding_balance"].astype(float)
        df["interest_rate"]         = df["interest_rate"].astype(float)
        df["fico_score"]            = df["fico_score"].astype(int)
        df["dti_ratio"]             = df["dti_ratio"].astype(float)
        return df

    def _credit_risk_tier(self, df):
        """
        Map FICO scores to credit risk tiers aligned with
        Basel III internal ratings-based (IRB) approach.
        """
        def tier(fico):
            if fico >= 780: return ("AAA", 0.003)
            elif fico >= 740: return ("AA",  0.006)
            elif fico >= 700: return ("A",   0.015)
            elif fico >= 660: return ("BBB", 0.035)
            elif fico >= 620: return ("BB",  0.075)
            elif fico >= 580: return ("B",   0.140)
            else:             return ("CCC", 0.280)

        tiers = df["fico_score"].apply(tier)
        df["credit_tier"]         = tiers.apply(lambda x: x[0])
        df["prob_of_default"]     = tiers.apply(lambda x: x[1])

        # Adjust PD upward for high DTI
        df["prob_of_default"] = df.apply(
            lambda r: min(0.95, r["prob_of_default"] * (1 + max(0, r["dti_ratio"] - 0.43) * 2)),
            axis=1
        ).round(4)
        return df

    def _ltv_risk_band(self, df):
        """Classify LTV risk for secured loans (QM and GSE thresholds)."""
        def ltv_band(ltv):
            if ltv is None or pd.isna(ltv): return "unsecured"
            if ltv <= 0.60: return "low_risk"
            elif ltv <= 0.80: return "standard"
            elif ltv <= 0.90: return "elevated"
            elif ltv <= 0.95: return "high_risk"
            else: return "non_conforming"

        df["ltv_risk_band"] = df["ltv_ratio"].apply(ltv_band)
        return df

    def _dti_classification(self, df):
        """
        Classify DTI per CFPB Qualified Mortgage (QM) rule.
        43% DTI = QM threshold. FHFA conforming limit for GSE purchase = 45%.
        """
        def dti_class(dti):
            if dti <= 0.28: return "conservative"
            elif dti <= 0.36: return "moderate"
            elif dti <= 0.43: return "at_qm_limit"
            elif dti <= 0.50: return "non_qm"
            else: return "distressed"

        df["dti_classification"] = df["dti_ratio"].apply(dti_class)
        return df

    def _expected_loss(self, df):
        """
        Expected Loss (EL) = PD × LGD × EAD
        LGD (Loss Given Default) varies by product and collateral.
        EAD = outstanding_balance (simplified; excludes off-balance exposure).
        """
        LGD_BY_TYPE = {
            "mortgage":       0.25,   # Low LGD — collateral-backed
            "heloc":          0.35,
            "auto":           0.40,
            "secured_credit": 0.45,
            "personal":       0.65,   # High LGD — unsecured
        }
        df["lgd"] = df["loan_type"].map(LGD_BY_TYPE)
        df["ead"] = df["outstanding_balance"]
        df["expected_loss"] = (df["prob_of_default"] *
                               df["lgd"] *
                               df["ead"]).round(2)
        df["el_rate"] = (df["expected_loss"] / df["ead"]).round(5)
        return df

    def _vintage_cohort(self, df):
        """Tag loans by origination year-quarter for cohort analysis."""
        df["origination_year"]    = df["origination_date"].dt.year
        df["origination_quarter"] = df["origination_date"].dt.to_period("Q").astype(str)
        df["loan_age_months"]     = (
            (pd.Timestamp.today() - df["origination_date"]) / pd.Timedelta(days=30.44)
        ).round(1)
        return df

    def _delinquency_score(self, df):
        """Numeric severity score for delinquency status."""
        SEVERITY = {
            "current":   0,
            "watchlist": 1,
            "30_dpd":    2,
            "60_dpd":    3,
            "90_dpd":    4,
            "default":   5,
        }
        df["delinquency_severity"] = df["loan_status"].map(SEVERITY).fillna(0).astype(int)
        df["is_delinquent"]        = (df["delinquency_severity"] >= 2).astype(int)
        df["is_default"]           = (df["loan_status"] == "default").astype(int)
        df["is_watchlist"]         = (df["loan_status"].isin(["watchlist", "30_dpd"])).astype(int)
        return df

    def _policy_exceptions(self, df):
        """
        Flag loans that breach underwriting policy thresholds.
        These typically require senior credit officer approval.
        """
        flags = pd.DataFrame(index=df.index)
        flags["exception_high_dti"]       = (df["dti_ratio"] > 0.50).astype(int)
        flags["exception_high_ltv"]       = (
            df["ltv_ratio"].notna() & (df["ltv_ratio"] > 0.95)
        ).astype(int)
        flags["exception_low_fico"]       = (df["fico_score"] < 600).astype(int)
        flags["exception_high_el_rate"]   = (df["el_rate"] > 0.05).astype(int)
        flags["exception_large_exposure"] = (df["outstanding_balance"] > 500_000).astype(int)

        df = pd.concat([df, flags], axis=1)
        df["exception_count"] = flags.sum(axis=1).astype(int)
        df["has_exception"]   = (df["exception_count"] > 0).astype(int)

        exc_rate = df["has_exception"].mean() * 100
        log.info(f"    Policy exception rate: {exc_rate:.1f}% of portfolio")
        return df

    def _revenue_metrics(self, df):
        """Estimate annualized interest revenue per loan."""
        df["annualized_interest_revenue"] = (
            df["outstanding_balance"] * df["interest_rate"]
        ).round(2)
        df["net_interest_margin_proxy"] = (
            df["interest_rate"] - df["el_rate"]
        ).round(5)
        return df

    def save_staging(self, df: pd.DataFrame):
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = STAGING_DIR / f"loans_staging_{ts}.csv"
        df.to_csv(path, index=False)
        log.info(f"  Staging data saved → {path}")
