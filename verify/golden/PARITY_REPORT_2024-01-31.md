# Reconciliation Report — banking_analytics / namespace `devin_nightly`

Business date `2024-01-31`, report month `202401`, raw source `banking_analytics.raw_sas`, generated 2026-09-18 20:40 UTC.

Source -> target controls proving the converted models match the legacy
Banking nightly batch. FAIL blocks the migration; SKIP means a prerequisite
(the golden seeds) is not loaded in this namespace.

| Family | Control | Result | Detail |
|---|---|---|---|
| completeness | `account_completeness` | PASS | in-scope raw accounts = 466, model accounts = 466 |
| completeness | `txn_feed_split` | PASS | feed = 622, validated = 610, rejected = 12 |
| control_total | `txn_feed_amount` | PASS | feed sum = 67444415.99, validated + rejected = 67444415.99 |
| completeness | `curated_append` | PASS | history 18293 + validated 610 = 18903, curated = 18903 |
| completeness | `running_balances_rows` | PASS | validated = 610, running_balances = 610 |
| completeness | `risk_scores_completeness` | PASS | lending accounts = 236, scored rows = 236, distinct accounts = 236 |
| completeness | `rwa_accounts` | PASS | month-end snapshot = 466, sum(N_ACCOUNTS) = 466 |
| control_total | `rwa_total_exposure` | PASS | snapshot balance = 37558944.21, sum(TOTAL_EXPOSURE) = 37558944.21 |
| control_total | `capital_total_rwa` | PASS | sum(RWA) = 14156900.58, CAPITAL_ADEQUACY.TOTAL_RWA = 14156900.58 |
| completeness | `delinquency_accounts` | PASS | lending accounts = 236, sum(N_ACCOUNTS) = 236 |
| control_total | `delinquency_total_balance` | PASS | lending balance = 29735012.62, sum(TOTAL_BALANCE) = 29735012.62 |
| completeness | `llp_loans` | PASS | lending accounts with loan detail = 236, sum(N_LOANS) = 236 |
| control_total | `llp_gross_loans` | PASS | expected = 29735012.62, sum(GROSS_LOANS) = 29735012.62 |
| mapping | `rwa_risk_weight_branches` | PASS | 12 (ACCOUNT_TYPE, RISK_WEIGHT) branches observed, 0 illegal |
| mapping | `delinquency_buckets` | PASS | buckets present in severity order: Current, 1-29, 30-59, 60-89, 90-119, 120-179, 180+ |
| mapping | `risk_rating_bands` | PASS | ratings observed: [3, 4]; rows outside their PD band: 0 |
| mapping | `capital_status_thresholds` | PASS | PASS/FAIL flags agree with 4.5% / 6.0% / 8.0% minimums |
| mapping | `acct_exception_codes` | PASS | exception rows by code: HIGH_UTIL=9, NEG_BAL=7, NO_RISK=16 |
| mapping | `anomaly_types` | PASS | anomalies by type: HIGH_AMOUNT=16, OVERDRAFT=30 |
| golden | `golden_cust_accounts_daily` | PASS | SAS rows = 466, dbt rows = 466, mismatched rows = 0 |
| golden | `golden_acct_exceptions` | PASS | SAS rows = 32, dbt rows = 32, accounts with different exception counts = 0 |
| golden | `golden_daily_transactions` | PASS | SAS rows = 18903, dbt rows = 18903, mismatched rows = 0 |
| golden | `golden_txn_anomalies` | PASS | SAS rows = 46, dbt rows = 46, mismatched rows = 0 |
| golden | `golden_running_balances` | PASS | SAS rows = 610, dbt rows = 610, mismatched rows = 0 |
| golden | `golden_risk_scores` | PASS | SAS rows = 236, dbt rows = 236, mismatched rows = 0 |
| golden | `golden_risk_migration` | PASS | SAS rows = 195, dbt rows = 195, mismatched rows = 0 |
| golden | `golden_risk_summary` | PASS | SAS rows = 12, dbt rows = 12, mismatched rows = 0 |
| golden | `golden_monthly_rwa` | PASS | SAS rows = 59, dbt rows = 59, mismatched rows = 0 |
| golden | `golden_delinquency_aging` | PASS | SAS rows = 70, dbt rows = 70, mismatched rows = 0 |
| golden | `golden_llp_coverage` | PASS | SAS rows = 6, dbt rows = 6, mismatched rows = 0 |
| golden | `golden_capital_adequacy` | PASS | SAS rows = 1, dbt rows = 1, mismatched rows = 0 |

**31 passed, 0 failed, 0 skipped**

## Golden data provenance

- Golden outputs come from running the legacy programs in the estate's container runtime (`opensas (container)`), estate `ts-sas-legacy-analytics` @ `ef511409f2bd93ff6304084edef8cc064ad7dad5`, business date `31JAN2024`.
- They are what the SAS code produces on the seeded estate — not an export from the bank's production SAS server. Re-generate with `docker compose -f docker/compose.yml run --rm sas` in the estate repo and `python seed/import_sas_golden.py`.
- SAS row counts: STG_BANK.CUST_ACCOUNTS_DAILY=466, STG_BANK.ACCT_EXCEPTIONS=32, CURATED.DAILY_TRANSACTIONS=18903, CURATED.TXN_ANOMALIES=46, CURATED.RISK_SCORES=236, REPORTS.MONTHLY_RWA=59, REPORTS.DELINQUENCY_AGING=70, REPORTS.LLP_COVERAGE=6.
- Known runtime deviation KD-001 (REPORTS.LLP_COVERAGE.NPL_COVERAGE_PCT): Model implements the SAS 9.4 formula. Golden parity accepts the model value on rows where the golden is 0 and NPL_BALANCE > 0 only if it equals TOTAL_ALLOWANCE / NPL_BALANCE * 100 computed from the golden's own figures.
