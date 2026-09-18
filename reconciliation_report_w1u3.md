# Reconciliation Report — banking_analytics / namespace `w1u3`

Raw source: `banking_analytics.raw_sas` · SAS golden: `verify/fixtures/sas_golden`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. an unconverted SAS program) has not been produced yet.

| SAS program | Control | Result | Detail |
|---|---|---|---|
| credit_risk_scoring.sas | `sas_control::risk_scores.n_accounts` | PASS | SAS = 236.0, Databricks = 236.0 |
| credit_risk_scoring.sas | `sas_rows::curated.risk_scores` | PASS | SAS CURATED.RISK_SCORES = 236, mart_risk_scores = 236 |
| daily_transaction_processing.sas | `sas_control::daily_transactions.sum_amount` | FAIL | SAS = 8231436.81, Databricks = 2179522.02 |
| daily_transaction_processing.sas | `sas_control::txn_anomalies.high_amount` | FAIL | SAS = 16.0, Databricks = 0.0 |
| daily_transaction_processing.sas | `sas_control::txn_anomalies.overdraft` | FAIL | SAS = 30.0, Databricks = 37.0 |
| daily_transaction_processing.sas | `sas_rows::curated.daily_transactions` | FAIL | SAS CURATED.DAILY_TRANSACTIONS = 18903, mart_daily_transactions = 610 |
| daily_transaction_processing.sas | `sas_rows::curated.txn_anomalies` | FAIL | SAS CURATED.TXN_ANOMALIES = 46, mart_transaction_anomalies = 37 |
| load_customer_accounts.sas | `account_completeness` | PASS | in-scope raw accounts = 466, model accounts = 466 |
| load_customer_accounts.sas | `sas_rows::stg_bank.acct_exceptions` | SKIP | SAS rows = 32; no converted model yet |
| load_customer_accounts.sas | `sas_rows::stg_bank.cust_accounts_daily` | PASS | SAS STG_BANK.CUST_ACCOUNTS_DAILY = 466, int_account_metrics = 466 |
| monthly_regulatory_reporting.sas | `sas_control::monthly_rwa.sum_rwa` | PASS | SAS = 14156900.58, Databricks = 14156900.58 |
| monthly_regulatory_reporting.sas | `sas_rows::reports.delinquency_aging` | PASS | SAS REPORTS.DELINQUENCY_AGING = 70, mart_delinquency_aging = 70 |
| monthly_regulatory_reporting.sas | `sas_rows::reports.llp_coverage` | PASS | SAS REPORTS.LLP_COVERAGE = 6, mart_llp_coverage = 6 |
| monthly_regulatory_reporting.sas | `sas_rows::reports.monthly_rwa` | PASS | SAS REPORTS.MONTHLY_RWA = 59, mart_regulatory_rwa = 59 |

**8 passed, 5 failed, 1 skipped**
