# SAS to dbt on Databricks — Migration Target Architecture

Target-state, **end-to-end runnable** migration of SAS analytics programs to dbt + Databricks. Each SAS program in [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics) has a corresponding dbt model with inline comments showing the translation from SAS constructs to SQL. The project also ships the executable spine the migration story needs: a synthetic-data seeder, a Databricks Asset Bundle (IaC), a CD deploy workflow, and one PySpark job demonstrating the alternative (imperative) migration path.

> **Running the full demo?** See [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) for the step-by-step, repeatable before→after walkthrough (seed → build → deploy → run → revert).

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
├── seed/
│   ├── generate_and_load.py      # Synthetic banking/insurance raw data seeder
│   └── teardown.py               # Drop a run's output schemas (raw untouched)
├── src/pyspark/
│   └── claims_processing.py      # PySpark port of claims_processing.sas (alternative path)
├── databricks.yml                # Databricks Asset Bundle (IaC) — variables + targets
├── resources/
│   └── daily_banking_pipeline.job.yml  # Workflow definition (dbt + PySpark tasks)
├── .github/workflows/
│   ├── dbt_ci.yml                # CI: sqlfluff lint, dbt parse, dbt test
│   └── deploy.yml                # CD: databricks bundle deploy (on main + manual)
├── docs/
│   ├── SAS_TO_DBT_MIGRATION_MAP.md  # Complete SAS→dbt construct mapping
│   └── DEMO_RUNBOOK.md           # Step-by-step end-to-end demo runbook
└── README.md
```

## Program-Level Migration Map

| SAS Source Program | dbt Model(s) | Migration Pattern |
|---|---|---|
| `load_customer_accounts.sas` | `stg_cust_accounts` → `int_account_metrics` | PROC SQL + DATA step → SQL + CASE |
| `daily_transaction_processing.sas` | `stg_daily_transactions` → `mart_daily_transactions` | RETAIN → window function |
| `credit_risk_scoring.sas` | `mart_risk_scores` | WOE scorecard → nested CASE + exp() |
| `monthly_regulatory_reporting.sas` | `mart_regulatory_rwa` + `mart_delinquency_aging` | PROC SQL aggregation → SQL GROUP BY |
| `claims_processing.sas` | `stg_claims` → `int_claims_adjudication` (dbt) **and** `src/pyspark/claims_processing.py` (PySpark) | Hash lookup → broadcast join |
| `policy_valuation.sas` | `int_policy_valuation` → `mart_loss_ratios` | MERGE BY → SQL JOIN |
| `customer_profitability.sas` | `mart_customer_pnl` | Multi-source merge → multi-ref JOIN |

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

### Seed Data + Run the dbt Project

```bash
pip install -r requirements.txt -r seed/requirements.txt
make seed                 # synthetic raw data -> banking_analytics.raw.*
make demo-up NS=dev       # dbt build (all models + tests) into dev_* schemas
```

### Deploy as IaC and Run the Workflow

```bash
make deploy               # databricks bundle deploy -t dev (schedule paused)
make run-job TARGET=dev   # run the deployed Workflow (dbt + PySpark tasks)
make destroy TARGET=dev   # revert: remove the deployed job
```

### Repeatable / Concurrent Runs

Every run writes to its own namespace, so the durable `banking_analytics.raw`
(before) data is never touched and concurrent runs never collide:

```bash
make demo-up   NS=alice   # builds alice_staging / _intermediate / _marts / _curated
make demo-down NS=alice   # drops only alice_* schemas
```

See [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) for the full before→after script.

## Related Repositories

| Repo | Purpose |
|---|---|
| [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics) | Source SAS estate (banking/insurance programs, macros, formats, batch orchestration) |
| [`uc-data-migration-sas-to-snowflake`](https://github.com/Cognition-Partner-Workshops/uc-data-migration-sas-to-snowflake) | Snowflake migration validation toolkit (lineage metadata, sample datasets, Streamlit UI) |
