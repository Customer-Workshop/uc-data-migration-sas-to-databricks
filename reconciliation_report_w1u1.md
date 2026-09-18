# Reconciliation Report — banking_analytics / namespace `w1u1`

Raw source: `banking_analytics.raw_sas` · SAS golden: `verify/fixtures/sas_golden`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. an unconverted SAS program) has not been produced yet.

| SAS program | Control | Result | Detail |
|---|---|---|---|
| credit_risk_scoring.sas | `sas_control::risk_scores.n_accounts` | PASS | SAS = 236.0, Databricks = 236.0 |
| credit_risk_scoring.sas | `sas_rows::curated.risk_scores` | PASS | SAS CURATED.RISK_SCORES = 236, mart_risk_scores = 236 |
| daily_transaction_processing.sas | `sas_control::daily_transactions.sum_amount` | PASS | SAS = 8231436.81, Databricks = 8231436.81 |
| daily_transaction_processing.sas | `sas_control::txn_anomalies.high_amount` | PASS | SAS = 16.0, Databricks = 16.0 |
| daily_transaction_processing.sas | `sas_control::txn_anomalies.overdraft` | PASS | SAS = 30.0, Databricks = 30.0 |
| daily_transaction_processing.sas | `sas_rows::curated.daily_transactions` | PASS | SAS CURATED.DAILY_TRANSACTIONS = 18903, mart_daily_transactions = 18903 |
| daily_transaction_processing.sas | `sas_rows::curated.txn_anomalies` | PASS | SAS CURATED.TXN_ANOMALIES = 46, mart_transaction_anomalies = 46 |
| load_customer_accounts.sas | `account_completeness` | PASS | in-scope raw accounts = 466, model accounts = 466 |
| load_customer_accounts.sas | `sas_rows::stg_bank.acct_exceptions` | SKIP | SAS rows = 32; no converted model yet |
| load_customer_accounts.sas | `sas_rows::stg_bank.cust_accounts_daily` | PASS | SAS STG_BANK.CUST_ACCOUNTS_DAILY = 466, int_account_metrics = 466 |
| monthly_regulatory_reporting.sas | `sas_control::monthly_rwa.sum_rwa` | SKIP | no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.delinquency_aging` | SKIP | SAS rows = 70; no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.llp_coverage` | SKIP | SAS rows = 6; no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.monthly_rwa` | SKIP | SAS rows = 59; no converted model yet |

**9 passed, 0 failed, 5 skipped**
