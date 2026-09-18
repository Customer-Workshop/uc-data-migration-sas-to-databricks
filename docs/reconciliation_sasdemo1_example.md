# Reconciliation Report — banking_analytics / namespace `sasdemo1` (source: `banking_analytics.raw_sas`)

Source -> target controls proving the converted marts match the legacy
SAS estate. FAIL blocks the migration; SKIP means the SAS output has no
converted model in this namespace yet.

SAS golden baseline: `./sas_golden` — estate `ts-sas-legacy-analytics` @ `1aa30f7`, runtime `opensas (container)`, business date `31JAN2024`, exported 2026-09-18T02:46:12Z.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 466, model accounts = 466 |
| `sas_rowcount:STG_BANK.CUST_ACCOUNTS_DAILY` | PASS | SAS STG_BANK.CUST_ACCOUNTS_DAILY = 466 rows, `stg_cust_accounts` = 466 rows |
| `sas_rowcount:STG_BANK.ACCT_EXCEPTIONS` | SKIP | load_customer_accounts.sas not yet converted: `banking_analytics.sasdemo1_staging.stg_acct_exceptions` does not exist (SAS rows = 32) |
| `sas_rowcount:CURATED.DAILY_TRANSACTIONS` | FAIL | SAS CURATED.DAILY_TRANSACTIONS = 18903 rows, `mart_daily_transactions` = 612 rows |
| `sas_rowcount:CURATED.TXN_ANOMALIES` | FAIL | SAS CURATED.TXN_ANOMALIES = 46 rows, `mart_transaction_anomalies` = 37 rows |
| `sas_rowcount:CURATED.RISK_SCORES` | PASS | SAS CURATED.RISK_SCORES = 236 rows, `mart_risk_scores` = 236 rows |
| `sas_rowcount:REPORTS.MONTHLY_RWA` | SKIP | monthly_regulatory_reporting.sas not yet converted: `banking_analytics.sasdemo1_marts.mart_regulatory_rwa` does not exist (SAS rows = 59) |
| `sas_rowcount:REPORTS.DELINQUENCY_AGING` | SKIP | monthly_regulatory_reporting.sas not yet converted: `banking_analytics.sasdemo1_marts.mart_delinquency_aging` does not exist (SAS rows = 70) |
| `sas_rowcount:REPORTS.LLP_COVERAGE` | SKIP | monthly_regulatory_reporting.sas not yet converted: `banking_analytics.sasdemo1_marts.mart_llp_coverage` does not exist (SAS rows = 6) |
| `sas_control:TXN_ANOMALIES.HIGH_AMOUNT` | FAIL | SAS = 16, target = 0 |
| `sas_control:TXN_ANOMALIES.OVERDRAFT` | FAIL | SAS = 30, target = 37 |
| `sas_control:DAILY_TRANSACTIONS.SUM_AMOUNT` | FAIL | SAS = 8,231,436.81, target = 2,179,743.61 |
| `sas_control:RISK_SCORES.N_ACCOUNTS` | PASS | SAS = 236, target = 236 |
| `sas_control:MONTHLY_RWA.SUM_RWA` | SKIP | REPORTS.MONTHLY_RWA not yet converted (SAS value = 14,156,900.58) |

**4 passed, 5 failed, 5 skipped**
