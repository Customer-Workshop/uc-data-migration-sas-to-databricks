# Reconciliation Report — banking_analytics / namespace `w1u2`

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
| load_customer_accounts.sas | `exception_completeness` | PASS | SAS rules on raw = 32, model exceptions = 32 |
| load_customer_accounts.sas | `exception_parity::high_util` | PASS | SAS rule = 9, model = 9 |
| load_customer_accounts.sas | `exception_parity::neg_bal` | PASS | SAS rule = 7, model = 7 |
| load_customer_accounts.sas | `exception_parity::no_risk` | PASS | SAS rule = 16, model = 16 |
| load_customer_accounts.sas | `sas_rows::stg_bank.acct_exceptions` | PASS | SAS STG_BANK.ACCT_EXCEPTIONS = 32, int_account_exceptions = 32 |
| load_customer_accounts.sas | `sas_rows::stg_bank.cust_accounts_daily` | PASS | SAS STG_BANK.CUST_ACCOUNTS_DAILY = 466, int_account_metrics = 466 |
| monthly_regulatory_reporting.sas | `sas_control::monthly_rwa.sum_rwa` | SKIP | no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.delinquency_aging` | SKIP | SAS rows = 70; no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.llp_coverage` | SKIP | SAS rows = 6; no converted model yet |
| monthly_regulatory_reporting.sas | `sas_rows::reports.monthly_rwa` | SKIP | SAS rows = 59; no converted model yet |

**9 passed, 5 failed, 4 skipped**
