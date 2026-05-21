# SAS to dbt on Databricks — Migration Target Architecture

Target-state dbt project for migrating SAS analytics programs to dbt + Databricks. Each SAS program in [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics) has a corresponding dbt model with inline comments showing the translation from SAS constructs to SQL.

## Repository Structure

```
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # Raw source → staging (replaces LIBNAME extracts)
│   │   ├── intermediate/         # Business logic (replaces DATA steps)
│   │   └── marts/                # Final outputs (replaces CURATED/REPORTS)
│   ├── macros/                   # PROC FORMAT → dbt Jinja macros
│   ├── dbt_project.yml           # Project config with Databricks profile
│   └── profiles.yml              # Databricks connection config (env-var-based)
├── docs/
│   └── SAS_TO_DBT_MIGRATION_MAP.md  # Complete SAS→dbt construct mapping
└── README.md
```

## Program-Level Migration Map

| SAS Source Program | dbt Model(s) | Migration Pattern |
|---|---|---|
| `load_customer_accounts.sas` | `stg_cust_accounts` → `int_account_metrics` | PROC SQL + DATA step → SQL + CASE |
| `daily_transaction_processing.sas` | `stg_daily_transactions` → `mart_daily_transactions` | RETAIN → window function |
| `credit_risk_scoring.sas` | `mart_risk_scores` | WOE scorecard → nested CASE + exp() |
| `monthly_regulatory_reporting.sas` | `mart_regulatory_rwa` + `mart_delinquency_aging` + `mart_llp_coverage` + `mart_capital_adequacy` | PROC SQL aggregation → SQL GROUP BY |
| `claims_processing.sas` | (planned) `stg_claims` → `int_claims_adjudication` | Hash lookup → broadcast join |
| `policy_valuation.sas` | (planned) `int_policy_valuation` → `mart_loss_ratios` | MERGE BY → SQL JOIN |
| `customer_profitability.sas` | (planned) `mart_customer_pnl` | Multi-source merge → multi-ref JOIN |

See [`docs/SAS_TO_DBT_MIGRATION_MAP.md`](docs/SAS_TO_DBT_MIGRATION_MAP.md) for the complete construct-level mapping (LIBNAME → Unity Catalog, PROC FORMAT → dbt macros, RETAIN → window functions, hash objects → broadcast joins, etc.).

## SAS Construct → dbt/Databricks Summary

```
SAS Environment                    dbt + Databricks
─────────────────────             ──────────────────────────
autoexec.sas (LIBNAMEs)    →     Unity Catalog + dbt sources
PROC FORMAT catalogs        →     dbt macros (CASE expressions)
SAS Macros (%macro)         →     dbt Jinja macros
DATA steps                  →     dbt SQL models (SELECT)
PROC SQL                    →     dbt SQL models (SELECT)
PROC MEANS / PROC FREQ      →     SQL GROUP BY aggregations
PROC APPEND                 →     dbt incremental models (MERGE)
%INCLUDE chains             →     dbt ref() DAG
Control-M scheduling        →     Databricks Workflows
Hash objects (lookup)       →     Spark broadcast joins
RETAIN + BY-group           →     Window functions (SUM OVER)
Macro variables (&var)      →     dbt vars / env_var()
```

## Quick Start

### Prerequisites

- Python 3.9+
- dbt-core + dbt-databricks (`pip install dbt-core dbt-databricks`)
- Databricks workspace with Unity Catalog enabled

### Configure Connection

Set environment variables for your Databricks workspace:

```bash
export DATABRICKS_HOST="adb-1234567890123456.7.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abcdef1234567890"
export DATABRICKS_TOKEN="dapi..."
```

### Run the dbt Project

```bash
cd dbt_project
dbt deps
dbt run --select tag:staging
dbt run --select tag:intermediate
dbt run --select tag:marts
```

## Related Repositories

| Repo | Purpose |
|---|---|
| [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics) | Source SAS estate (banking/insurance programs, macros, formats, batch orchestration) |
| [`uc-data-migration-sas-to-snowflake`](https://github.com/Cognition-Partner-Workshops/uc-data-migration-sas-to-snowflake) | Snowflake migration validation toolkit (lineage metadata, sample datasets, Streamlit UI) |
