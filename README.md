# Loan Portfolio Risk Data Pipeline

> **An end-to-end ETL pipeline for consumer and commercial loan portfolio risk analytics — modeled after workflows at JPMorgan, Wells Fargo, and SoFi.**

---

## Overview

This pipeline ingests raw loan-level data from a lending book (mortgages, personal, auto, HELOC, secured credit), runs it through a rigorous data quality gate, enriches each loan with risk metrics (PD, LGD, EAD, Expected Loss, policy exceptions), and loads a star-schema warehouse with executive-level portfolio risk dashboards and concentration analysis.

---

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐    ┌──────────────┐
│    INGEST    │───▶│  QC / GATE   │───▶│  RISK TRANSFORM      │───▶│     LOAD     │
│              │    │              │    │                      │    │              │
│ Core Banking │    │ 9 automated  │    │ PD Tiers (Basel IRB) │    │ SQLite /     │
│ LOS / Broker │    │ DQ checks:   │    │ LTV Risk Bands       │    │ DuckDB /     │
│ Data Lake    │    │ Schema,      │    │ DTI / QM Rules       │    │ Snowflake    │
│ (simulated)  │    │ FICO, DTI,   │    │ Expected Loss (EL)   │    │              │
│              │    │ Rates, IDs   │    │ Policy Exceptions    │    │ Star Schema  │
└──────────────┘    └──────────────┘    │ Vintage Cohorts      │    │ 7 Tables     │
                                        │ Revenue / NIM        │    └──────────────┘
                                        └──────────────────────┘
```

---

## Warehouse Schema

| Table | Type | Description |
|---|---|---|
| `fact_loans` | Fact | Loan-level record with 40+ risk features |
| `dim_loan_types` | Dimension | Product metadata and collateral type |
| `dim_credit_tiers` | Dimension | Basel IRB credit tier reference |
| `risk_portfolio_summary` | Aggregate | Top-level portfolio risk dashboard |
| `risk_by_product` | Aggregate | Risk breakdown by loan product |
| `risk_by_credit_tier` | Aggregate | EL, PD, delinquency by credit tier |
| `risk_by_vintage` | Aggregate | Cohort roll-rate and delinquency analysis |
| `risk_concentration` | Aggregate | Concentration risk by state, product, tier |

---

## Risk Metrics Computed

**Credit Risk (Basel III IRB-aligned)**
- Probability of Default (PD) — FICO-based with DTI stress adjustment
- Loss Given Default (LGD) — product-type calibrated
- Exposure at Default (EAD) — outstanding balance
- Expected Loss (EL = PD × LGD × EAD)
- Credit tier (AAA → CCC)

**Collateral Risk**
- LTV risk band (low_risk / standard / elevated / high_risk / non_conforming)
- CFPB Qualified Mortgage (QM) DTI rule classification

**Portfolio Risk**
- Delinquency rate by product, tier, vintage, state
- Default rate
- Vintage cohort analysis (origination quarter roll-rates)
- Concentration risk by state, product, credit tier

**Policy Exceptions**
- DTI > 50% (non-QM distressed)
- LTV > 95% (non-conforming)
- FICO < 600 (deep sub-prime threshold)
- EL rate > 5% (elevated credit cost)
- Large exposure > $500K

**Revenue Metrics**
- Annualized interest revenue per loan
- Net Interest Margin proxy (rate − EL rate)

---

## Getting Started

### Requirements
```bash
pip install pandas
```

### Run
```bash
python pipeline.py
```

### Configure
Edit `NUM_LOANS` and `RANDOM_SEED` in `pipeline.py`.
Increase to 50,000+ to simulate a production-scale book.

---

## Swapping to Real Data

**From database:**
```python
import pandas as pd, sqlalchemy
engine = sqlalchemy.create_engine("postgresql://user:pass@host/db")
df = pd.read_sql("SELECT * FROM loan_originations WHERE status != 'paid_off'", engine)
```

**From data lake (S3/ADLS):**
```python
df = pd.read_parquet("s3://lending-datalake/loans/active/")
# or: df = pd.read_parquet("abfss://container@storage.dfs.core.windows.net/loans/")
```

**Load to Snowflake:**
```python
from snowflake.connector.pandas_tools import write_pandas
write_pandas(snowflake_conn, df, "FACT_LOANS", database="RISK_DW", schema="LENDING")
```

---

## Project Structure

```
loan_risk_pipeline/
├── pipeline.py           # Orchestrator
├── loan_generator.py     # Stage 1: Synthetic loan book (swap for real source)
├── loan_quality.py       # Stage 2: 9-check DQ gate
├── loan_transformer.py   # Stage 3: Risk scoring & enrichment
├── loan_loader.py        # Stage 4: Star schema warehouse load
├── data/
│   ├── raw/              # Raw ingested files
│   ├── staging/          # Transformed / scored files
│   └── warehouse/        # SQLite warehouse (loan_warehouse.db)
└── logs/                 # Timestamped run logs
```

---

## Skills Demonstrated

- End-to-end financial data pipeline design
- Credit risk modeling (Basel III IRB PD/LGD/EAD framework)
- Regulatory rule implementation (CFPB QM, FHFA LTV thresholds)
- Star schema dimensional modeling for risk analytics
- Data quality framework with business rule validation
- Vintage cohort analysis and roll-rate methodology
- Concentration risk aggregation
- Policy exception detection and flagging
- Modular, production-swap-ready architecture
