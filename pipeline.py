"""
============================================================
  Loan Portfolio Risk Data Pipeline
  Author: Calvin White
  Description:
    End-to-end ETL pipeline that ingests raw loan-level data
    from a simulated lending book (mortgages, personal loans,
    auto loans, secured lending), applies risk scoring
    transformations (LTV, DTI, PD tiers), flags anomalies
    and policy violations, and loads to a star-schema
    warehouse with executive-level risk summary tables.

    Modeled after risk data workflows at banks and
    lending institutions (JPMorgan, Wells Fargo, SoFi, etc.)
============================================================
"""

import logging
import time
from datetime import datetime
from pathlib import Path

from loan_generator  import LoanDataGenerator
from loan_quality    import LoanQualityChecker
from loan_transformer import LoanTransformer
from loan_loader     import LoanWarehouseLoader

# ── Logging ───────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"loan_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("loan_pipeline.orchestrator")

# ── Config ────────────────────────────────────────────────
NUM_LOANS     = 5_000    # Size of synthetic loan book
RANDOM_SEED   = 42


def run_pipeline():
    start = time.time()
    log.info("=" * 60)
    log.info("Loan Portfolio Risk Pipeline — START")
    log.info(f"Portfolio size: {NUM_LOANS:,} loans")
    log.info("=" * 60)

    # ── Stage 1: Ingest ───────────────────────────────────
    log.info("[STAGE 1] Generating / ingesting raw loan data...")
    generator = LoanDataGenerator(n=NUM_LOANS, seed=RANDOM_SEED)
    raw_df = generator.generate()
    generator.save_raw(raw_df)
    log.info(f"  Ingested {len(raw_df):,} loan records")
    log.info(f"  Product mix: {raw_df['loan_type'].value_counts().to_dict()}")

    # ── Stage 2: Data Quality ─────────────────────────────
    log.info("[STAGE 2] Running data quality checks...")
    checker = LoanQualityChecker(raw_df)
    report  = checker.run_all_checks()
    checker.log_report(report)
    if not report["passed"]:
        log.error("  Data quality gate FAILED — aborting.")
        raise RuntimeError("Loan data quality check failed.")
    log.info("  All quality checks PASSED ✓")

    # ── Stage 3: Risk Transformation ─────────────────────
    log.info("[STAGE 3] Applying risk scoring and transformations...")
    transformer = LoanTransformer(raw_df)
    staged_df   = transformer.transform()
    transformer.save_staging(staged_df)
    log.info(f"  Enriched {len(staged_df):,} records with {len(staged_df.columns)} columns")

    # ── Stage 4: Load to Warehouse ────────────────────────
    log.info("[STAGE 4] Loading to SQLite data warehouse...")
    loader = LoanWarehouseLoader()
    loader.load_fact_loans(staged_df)
    loader.build_dim_tables(staged_df)
    loader.build_risk_aggregates(staged_df)
    loader.print_portfolio_summary()

    elapsed = round(time.time() - start, 2)
    log.info("=" * 60)
    log.info(f"Pipeline completed successfully in {elapsed}s")
    log.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
