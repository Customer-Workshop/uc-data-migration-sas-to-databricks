# SAS Library → dbt/Databricks Schema Mapping

> **Phase 0.6** of the Consolidated Migration Assessment.
> Maps every SAS LIBNAME from `autoexec.sas` to its Databricks Unity Catalog / dbt target schema.

## Library Mapping

| SAS LIBNAME | SAS Physical Path / Connection | Databricks Catalog.Schema | dbt Layer | dbt `source` / `schema` |
|---|---|---|---|---|
| `RAW_BANK` | `/data/sas/raw/banking` (read-only) | `banking_analytics.raw` | source | `banking_raw` |
| `RAW_INS` | `/data/sas/raw/insurance` (read-only) | `banking_analytics.raw` | source | `insurance_raw` |
| `ORA_DW` | Oracle `FINPROD.DW_BANKING` (read-only) | `banking_analytics.raw` | source | `banking_raw` (tables landed via ingestion) |
| `TERA_DW` | Teradata `ANALYTICS` (read-only) | `banking_analytics.raw` | source | `insurance_raw` (tables landed via ingestion) |
| `STG_BANK` | `/data/sas/staging/banking` | `banking_analytics.staging` | staging | `+schema: staging` |
| `STG_INS` | `/data/sas/staging/insurance` | `banking_analytics.staging` | staging | `+schema: staging` |
| `CURATED` | `/data/sas/curated` | `banking_analytics.intermediate` | intermediate | `+schema: intermediate` |
| `REPORTS` | `/data/sas/reports` | `banking_analytics.marts` | marts | `+schema: marts` |
| `ARCHIVE` | `/data/sas/archive` | `banking_analytics.archive` | — | Out of scope (cold storage / Delta time-travel) |
| `BANKING` | `/data/sas/formats/banking` (format catalog) | — | — | Replaced by dbt macros (`macros/format_*.sql`) |
| `INSURANCE` | `/data/sas/formats/insurance` (format catalog) | — | — | Replaced by dbt macros (`macros/format_*.sql`) |
| `COMMON` | `/data/sas/formats/common` (format catalog) | — | — | Replaced by dbt macros (if any formats used) |

## Source Table Mapping

### Banking Domain (`banking_raw` source → `banking_analytics.raw`)

| SAS Dataset | SAS Source | dbt Source Table | Notes |
|---|---|---|---|
| `ORA_DW.CUST_ACCOUNTS` | Oracle DW | `banking_raw.cust_accounts` | Landed via JDBC ingestion into Delta |
| `ORA_DW.CUST_DEMOGRAPHICS` | Oracle DW | `banking_raw.cust_demographics` | Landed via JDBC ingestion into Delta |
| `ORA_DW.BUREAU_SCORES` | Oracle DW | `banking_raw.bureau_scores` | Landed via JDBC ingestion into Delta |
| `ORA_DW.PAYMENT_HISTORY` | Oracle DW | `banking_raw.payment_history` | Landed via JDBC ingestion into Delta |
| `ORA_DW.COLLATERAL` | Oracle DW | `banking_raw.collateral` | Landed via JDBC ingestion into Delta |
| `ORA_DW.LOAN_DETAILS` | Oracle DW | `banking_raw.loan_details` | Landed via JDBC ingestion into Delta |
| `RAW_BANK.TXN_FEED_YYYYMMDD` | File-based (date-partitioned) | `banking_raw.daily_transactions` | SAS dynamic names → single Delta table with `transaction_date` partition |

### Insurance Domain (`insurance_raw` source → `banking_analytics.raw`)

| SAS Dataset | SAS Source | dbt Source Table | Notes |
|---|---|---|---|
| `RAW_INS.POLICIES` | File-based | `insurance_raw.policies` | Policy master |
| `RAW_INS.CLAIMS` | File-based | `insurance_raw.claims` | Claims register |
| `RAW_INS.PREMIUMS` | File-based | `insurance_raw.premiums` | Premium payments |
| `TERA_DW.FRAUD_INDICATORS` | Teradata | `insurance_raw.fraud_indicators` | Landed via JDBC ingestion into Delta |

## Output Table Mapping

### Staging Layer (`+schema: staging`)

| SAS Output Dataset | dbt Model | Materialization |
|---|---|---|
| `STG_BANK.CUST_ACCOUNTS_DAILY` | `stg_cust_accounts` | view |
| `WORK.TXN_VALIDATED` (transient) | `stg_daily_transactions` | view |
| `STG_INS.CLAIMS_REGISTER` | `stg_claims` (planned) | view |

### Intermediate Layer (`+schema: intermediate`)

| SAS Output Dataset | dbt Model | Materialization |
|---|---|---|
| `STG_BANK.CUST_ACCOUNTS_DAILY` (enriched) | `int_account_metrics` | table |
| `STG_INS.CLAIMS_REVIEW_QUEUE` | `int_claims_adjudication` (planned) | table |
| `STG_INS.POLICY_VALUATION` | `int_policy_valuation` (planned) | table |

### Marts Layer (`+schema: marts`)

| SAS Output Dataset | dbt Model | Materialization |
|---|---|---|
| `CURATED.DAILY_TRANSACTIONS` | `mart_daily_transactions` | incremental (merge) |
| `CURATED.TXN_ANOMALIES` | `mart_transaction_anomalies` | table |
| `CURATED.RISK_SCORES` | `mart_risk_scores` | table |
| `REPORTS.MONTHLY_RWA` | `mart_regulatory_rwa` (planned) | table |
| `REPORTS.DELINQUENCY_AGING` | `mart_delinquency_aging` (planned) | table |
| `REPORTS.LOSS_RATIO_SUMMARY` | `mart_loss_ratios` (planned) | table |
| `REPORTS.CUSTOMER_PNL` | `mart_customer_pnl` (planned) | table |

## SAS Macro Variable → dbt Variable Mapping

| SAS Macro Variable | Source | dbt Variable | dbt Source |
|---|---|---|---|
| `&ENVIRONMENT` | `autoexec.sas` | `var('environment')` | `dbt_project.yml` / `--vars` |
| `&CURR_DT` | `%sysfunc(today(), date9.)` | `var('curr_dt')` | Jinja `run_started_at` |
| `&PREV_YM` | `%sysfunc(intnx(month, ...))` | `var('prev_ym')` | Jinja date math |
| `&MAX_OBS_WARN` | `autoexec.sas` | `var('max_obs_warn')` | `dbt_project.yml` |
| `&BASE_PATH` | `autoexec.sas` | — | Replaced by Unity Catalog paths |
| `&LOG_PATH` | `autoexec.sas` | — | Replaced by Databricks job logs |
| `&REPORT_PATH` | `autoexec.sas` | — | Replaced by Unity Catalog volumes |
| `&EMAIL_DL` | `autoexec.sas` | — | Phase 5: Databricks Alerts / webhook |
| `&ABORT_ON_ERR` | `autoexec.sas` | — | dbt `severity: error` on tests |

## Format Catalog → dbt Macro Mapping

### Banking Formats (`Formats/banking_formats.sas`)

| SAS Format | Type | dbt Macro | Status |
|---|---|---|---|
| `$ACCTTYPE` | Character | `format_account_type` | Migrated |
| `$ACCTSTAT` | Character | `format_account_status` | Migrated |
| `RISKRATE` | Numeric | `format_risk_rating` | Migrated |
| `$TXNCAT` | Character | `format_txn_category` | Migrated |
| `DELQBKT` | Numeric (range) | `format_delinquency_bucket` | Migrated |
| `BALRANGE` | Numeric (range) | `format_balance_range` | Migrated |
| `$REGION` | Character | `format_region` | Migrated |
| `$CUSTSEG` | Character | `format_customer_segment` | Migrated |
| `$LNPURP` | Character | `format_loan_purpose` | Migrated |

### Insurance Formats (`Formats/insurance_formats.sas`)

| SAS Format | Type | dbt Macro | Status |
|---|---|---|---|
| `$POLTYPE` | Character | `format_policy_type` | Migrated |
| `$CLMSTAT` | Character | `format_claim_status` | Migrated |
| `$RISKCAT` | Character | `format_risk_category` | Migrated |
| `$COVTYPE` | Character | `format_coverage_type` | Migrated |
| `LOSSRANGE` | Numeric (range) | `format_loss_range` | Migrated |

## Shared Macro → dbt Utility Mapping

| SAS Macro | Used By | dbt Equivalent | Status |
|---|---|---|---|
| `%parmv` | All 7 programs | `validate_required_var` | Migrated |
| `%nobs` | All 7 programs | `row_count` / `log_row_count` | Migrated |
| `%sendmail` | 2 programs | — | Phase 5 (Databricks Alerts) |
| `%lock` | 2 programs | — | Phase 5 (database transactions) |
| `%export_xlsx` | 2 programs | — | Phase 5 (Databricks notebooks) |

## Connectivity Notes

### Phase 0.4: Oracle DW → Databricks (Pending)

SAS connects to Oracle via `SAS/ACCESS` LIBNAME. In Databricks, Oracle data
is landed into Unity Catalog via one of:
- **Databricks JDBC connector** (Spark `read.format("jdbc")`)
- **Fivetran / Airbyte** managed ingestion
- **Delta Live Tables** with JDBC source

The raw tables in `banking_analytics.raw` must be populated before dbt models
can run. This is an infrastructure prerequisite outside the dbt project scope.

### Phase 0.5: Teradata → Databricks (Pending)

SAS connects to Teradata via `SAS/ACCESS` LIBNAME. Same landing pattern as
Oracle — data ingested into `banking_analytics.raw` via JDBC or managed ETL.
Currently only `TERA_DW.FRAUD_INDICATORS` is actively used by the 7 programs.
