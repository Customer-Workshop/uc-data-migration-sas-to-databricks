# Reconciliation Report — banking_analytics / namespace `session1`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 469, model accounts = 469 |
| `rwa_completeness` | PASS | source accounts = 469, mart sum(n_accounts) = 469 |
| `rwa_exposure_control_total` | PASS | source balance = 93223155.41999999, mart exposure = 93223155.41999996, diff = 0.00 |
| `rwa_risk_weight_coverage` | PASS | 11 combos checked, 0 violations: none |
| `delinquency_completeness` | PASS | source lending accounts = 252, mart sum(n_accounts) = 252 |
| `delinquency_balance_control_total` | PASS | source balance = 49607451.61999999, mart balance = 49607451.62, diff = 0.00 |
| `delinquency_bucket_parity` | PASS | 7 distinct buckets, 0 invalid: none |

**7 passed, 0 failed, 0 skipped**
