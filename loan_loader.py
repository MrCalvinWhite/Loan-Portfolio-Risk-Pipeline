"""
loan_loader.py
--------------
Loads enriched loan data into a SQLite data warehouse.

Schema:
  fact_loans                — Loan-level fact table with all risk metrics
  dim_loan_types            — Product dimension
  dim_credit_tiers          — Credit tier reference
  dim_states                — State dimension
  risk_portfolio_summary    — Top-level portfolio risk dashboard
  risk_by_product           — Risk breakdown by loan type
  risk_by_credit_tier       — Risk breakdown by credit rating
  risk_by_vintage           — Cohort/vintage roll-rate analysis
  risk_concentration        — Concentration risk (state, product, tier)
"""

import sqlite3
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

log = logging.getLogger("loan_pipeline.loader")

WAREHOUSE_DIR = Path("data/warehouse")
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = WAREHOUSE_DIR / "loan_warehouse.db"


class LoanWarehouseLoader:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        log.info(f"  Connected to warehouse: {DB_PATH}")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS fact_loans (
                loan_id                     TEXT PRIMARY KEY,
                loan_type                   TEXT,
                origination_date            TEXT,
                origination_year            INTEGER,
                origination_quarter         TEXT,
                loan_age_months             REAL,
                state                       TEXT,
                source_system               TEXT,
                principal                   REAL,
                outstanding_balance         REAL,
                interest_rate               REAL,
                term_months                 INTEGER,
                monthly_payment             REAL,
                annual_income               REAL,
                fico_score                  INTEGER,
                dti_ratio                   REAL,
                ltv_ratio                   REAL,
                property_value              REAL,
                loan_status                 TEXT,
                delinquency_severity        INTEGER,
                is_delinquent               INTEGER,
                is_default                  INTEGER,
                is_watchlist                INTEGER,
                credit_tier                 TEXT,
                prob_of_default             REAL,
                lgd                         REAL,
                ead                         REAL,
                expected_loss               REAL,
                el_rate                     REAL,
                ltv_risk_band               TEXT,
                dti_classification          TEXT,
                annualized_interest_revenue REAL,
                net_interest_margin_proxy   REAL,
                exception_count             INTEGER,
                has_exception               INTEGER,
                exception_high_dti          INTEGER,
                exception_high_ltv          INTEGER,
                exception_low_fico          INTEGER,
                exception_high_el_rate      INTEGER,
                exception_large_exposure    INTEGER,
                pipeline_version            TEXT,
                loaded_at                   TEXT
            );

            CREATE TABLE IF NOT EXISTS risk_portfolio_summary (
                snapshot_date               TEXT PRIMARY KEY,
                total_loans                 INTEGER,
                total_outstanding           REAL,
                total_expected_loss         REAL,
                portfolio_el_rate           REAL,
                avg_fico                    REAL,
                avg_dti                     REAL,
                avg_ltv                     REAL,
                delinquency_rate            REAL,
                default_rate                REAL,
                exception_rate              REAL,
                annualized_revenue          REAL,
                avg_nim_proxy               REAL
            );

            CREATE TABLE IF NOT EXISTS risk_by_product (
                loan_type           TEXT PRIMARY KEY,
                loan_count          INTEGER,
                total_outstanding   REAL,
                total_el            REAL,
                el_rate             REAL,
                avg_fico            REAL,
                avg_dti             REAL,
                delinquency_rate    REAL,
                default_rate        REAL,
                avg_rate            REAL,
                pct_of_portfolio    REAL,
                loaded_at           TEXT
            );

            CREATE TABLE IF NOT EXISTS risk_by_credit_tier (
                credit_tier         TEXT PRIMARY KEY,
                loan_count          INTEGER,
                total_outstanding   REAL,
                avg_pd              REAL,
                total_el            REAL,
                delinquency_rate    REAL,
                default_rate        REAL,
                avg_nim             REAL,
                loaded_at           TEXT
            );

            CREATE TABLE IF NOT EXISTS risk_by_vintage (
                origination_quarter TEXT PRIMARY KEY,
                loan_count          INTEGER,
                total_outstanding   REAL,
                avg_fico            REAL,
                avg_dti             REAL,
                delinquency_rate    REAL,
                default_rate        REAL,
                total_el            REAL,
                loaded_at           TEXT
            );

            CREATE TABLE IF NOT EXISTS risk_concentration (
                dimension           TEXT,
                value               TEXT,
                loan_count          INTEGER,
                total_outstanding   REAL,
                pct_of_portfolio    REAL,
                delinquency_rate    REAL,
                loaded_at           TEXT,
                PRIMARY KEY (dimension, value)
            );
        """)
        self.conn.commit()

    def load_fact_loans(self, df: pd.DataFrame):
        load_df = df.copy()
        load_df["origination_date"] = load_df["origination_date"].astype(str)
        load_df["loaded_at"]        = datetime.utcnow().isoformat()
        load_df.to_sql("fact_loans", self.conn, if_exists="replace", index=False)
        log.info(f"  Loaded {len(load_df):,} rows → fact_loans")

    def build_dim_tables(self, df: pd.DataFrame):
        # dim_loan_types
        pd.DataFrame({
            "loan_type":    ["mortgage", "personal", "auto", "secured_credit", "heloc"],
            "description":  ["Residential mortgage", "Unsecured personal loan",
                             "Auto/vehicle loan", "Secured credit/LOC", "Home equity line of credit"],
            "collateral":   ["Real estate", "None", "Vehicle", "Financial assets", "Real estate"],
            "is_secured":   [1, 0, 1, 1, 1],
        }).to_sql("dim_loan_types", self.conn, if_exists="replace", index=False)

        # dim_credit_tiers
        pd.DataFrame({
            "credit_tier": ["AAA","AA","A","BBB","BB","B","CCC"],
            "fico_min":    [780, 740, 700, 660, 620, 580, 300],
            "fico_max":    [850, 779, 739, 699, 659, 619, 579],
            "risk_label":  ["Prime Plus","Prime","Near-Prime","Standard",
                            "Sub-Prime","Deep Sub-Prime","Non-Prime"],
            "base_pd":     [0.003, 0.006, 0.015, 0.035, 0.075, 0.140, 0.280],
        }).to_sql("dim_credit_tiers", self.conn, if_exists="replace", index=False)

        log.info("  Loaded dimension tables → dim_loan_types, dim_credit_tiers")

    def build_risk_aggregates(self, df: pd.DataFrame):
        total_outstanding = df["outstanding_balance"].sum()

        # ── Portfolio Summary ─────────────────────────────
        pd.DataFrame([{
            "snapshot_date":       datetime.today().strftime("%Y-%m-%d"),
            "total_loans":         len(df),
            "total_outstanding":   round(total_outstanding, 2),
            "total_expected_loss": round(df["expected_loss"].sum(), 2),
            "portfolio_el_rate":   round(df["expected_loss"].sum() / total_outstanding, 5),
            "avg_fico":            round(df["fico_score"].mean(), 1),
            "avg_dti":             round(df["dti_ratio"].mean(), 4),
            "avg_ltv":             round(df["ltv_ratio"].mean(), 4),
            "delinquency_rate":    round(df["is_delinquent"].mean(), 4),
            "default_rate":        round(df["is_default"].mean(), 4),
            "exception_rate":      round(df["has_exception"].mean(), 4),
            "annualized_revenue":  round(df["annualized_interest_revenue"].sum(), 2),
            "avg_nim_proxy":       round(df["net_interest_margin_proxy"].mean(), 5),
        }]).to_sql("risk_portfolio_summary", self.conn, if_exists="replace", index=False)

        # ── By Product ────────────────────────────────────
        by_product = []
        for lt, g in df.groupby("loan_type"):
            by_product.append({
                "loan_type":         lt,
                "loan_count":        len(g),
                "total_outstanding": round(g["outstanding_balance"].sum(), 2),
                "total_el":          round(g["expected_loss"].sum(), 2),
                "el_rate":           round(g["el_rate"].mean(), 5),
                "avg_fico":          round(g["fico_score"].mean(), 1),
                "avg_dti":           round(g["dti_ratio"].mean(), 4),
                "delinquency_rate":  round(g["is_delinquent"].mean(), 4),
                "default_rate":      round(g["is_default"].mean(), 4),
                "avg_rate":          round(g["interest_rate"].mean(), 5),
                "pct_of_portfolio":  round(g["outstanding_balance"].sum() / total_outstanding, 4),
                "loaded_at":         datetime.utcnow().isoformat(),
            })
        pd.DataFrame(by_product).to_sql("risk_by_product", self.conn, if_exists="replace", index=False)

        # ── By Credit Tier ────────────────────────────────
        tier_order = ["AAA","AA","A","BBB","BB","B","CCC"]
        by_tier = []
        for tier, g in df.groupby("credit_tier"):
            by_tier.append({
                "credit_tier":       tier,
                "loan_count":        len(g),
                "total_outstanding": round(g["outstanding_balance"].sum(), 2),
                "avg_pd":            round(g["prob_of_default"].mean(), 5),
                "total_el":          round(g["expected_loss"].sum(), 2),
                "delinquency_rate":  round(g["is_delinquent"].mean(), 4),
                "default_rate":      round(g["is_default"].mean(), 4),
                "avg_nim":           round(g["net_interest_margin_proxy"].mean(), 5),
                "loaded_at":         datetime.utcnow().isoformat(),
            })
        pd.DataFrame(by_tier).to_sql("risk_by_credit_tier", self.conn, if_exists="replace", index=False)

        # ── By Vintage ────────────────────────────────────
        by_vintage = []
        for qtr, g in df.groupby("origination_quarter"):
            by_vintage.append({
                "origination_quarter": str(qtr),
                "loan_count":          len(g),
                "total_outstanding":   round(g["outstanding_balance"].sum(), 2),
                "avg_fico":            round(g["fico_score"].mean(), 1),
                "avg_dti":             round(g["dti_ratio"].mean(), 4),
                "delinquency_rate":    round(g["is_delinquent"].mean(), 4),
                "default_rate":        round(g["is_default"].mean(), 4),
                "total_el":            round(g["expected_loss"].sum(), 2),
                "loaded_at":           datetime.utcnow().isoformat(),
            })
        pd.DataFrame(by_vintage).to_sql("risk_by_vintage", self.conn, if_exists="replace", index=False)

        # ── Concentration Risk ────────────────────────────
        conc_rows = []
        for dim, col in [("state", "state"), ("product", "loan_type"), ("credit_tier", "credit_tier")]:
            for val, g in df.groupby(col):
                conc_rows.append({
                    "dimension":         dim,
                    "value":             val,
                    "loan_count":        len(g),
                    "total_outstanding": round(g["outstanding_balance"].sum(), 2),
                    "pct_of_portfolio":  round(g["outstanding_balance"].sum() / total_outstanding, 4),
                    "delinquency_rate":  round(g["is_delinquent"].mean(), 4),
                    "loaded_at":         datetime.utcnow().isoformat(),
                })
        pd.DataFrame(conc_rows).to_sql("risk_concentration", self.conn, if_exists="replace", index=False)

        log.info("  Built risk aggregates → portfolio_summary, by_product, by_credit_tier, by_vintage, concentration")

    def print_portfolio_summary(self):
        row = self.conn.execute("SELECT * FROM risk_portfolio_summary").fetchone()
        log.info("  ── Portfolio Risk Dashboard ─────────────────")
        log.info(f"    Total Loans:           {row[1]:>10,}")
        log.info(f"    Total Outstanding:     ${row[2]:>14,.0f}")
        log.info(f"    Total Expected Loss:   ${row[3]:>14,.0f}")
        log.info(f"    Portfolio EL Rate:     {row[4]*100:>10.2f}%")
        log.info(f"    Avg FICO Score:        {row[5]:>10.0f}")
        log.info(f"    Avg DTI Ratio:         {row[6]*100:>10.1f}%")
        log.info(f"    Delinquency Rate:      {row[8]*100:>10.2f}%")
        log.info(f"    Default Rate:          {row[9]*100:>10.2f}%")
        log.info(f"    Exception Rate:        {row[10]*100:>10.2f}%")
        log.info(f"    Annualized Revenue:    ${row[11]:>14,.0f}")
        log.info(f"    Avg NIM Proxy:         {row[12]*100:>10.2f}%")

        log.info("  ── Risk by Product ─────────────────────────")
        rows = self.conn.execute("""
            SELECT loan_type, loan_count, total_outstanding, el_rate, delinquency_rate, default_rate
            FROM risk_by_product ORDER BY total_outstanding DESC
        """).fetchall()
        for r in rows:
            log.info(f"    {r[0]:<16} {r[1]:>5,} loans  ${r[2]:>12,.0f}  "
                     f"EL={r[3]*100:.2f}%  DQ={r[4]*100:.1f}%  Def={r[5]*100:.1f}%")

        log.info("  ── Risk by Credit Tier ──────────────────────")
        rows = self.conn.execute("""
            SELECT credit_tier, loan_count, avg_pd, total_el, delinquency_rate
            FROM risk_by_credit_tier ORDER BY avg_pd
        """).fetchall()
        for r in rows:
            log.info(f"    {r[0]:<5}  {r[1]:>5,} loans  PD={r[2]*100:.1f}%  "
                     f"EL=${r[3]:>10,.0f}  DQ={r[4]*100:.1f}%")
