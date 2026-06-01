# Reconciliation Report — banking_analytics / namespace `apidemo1`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 468, model accounts = 468 |
| `rwa_completeness` | PASS | source accounts = 468, mart accounts = 468 |
| `rwa_control_total` | PASS | source balance = 94961804.93999995, mart total_exposure = 94961804.94000003, diff = 0.00 |
| `rwa_risk_weight_parity` | PASS | all account_type weights match SAS mapping |
| `delinquency_completeness` | PASS | credit-product accounts = 249, mart accounts = 249 |
| `delinquency_control_total` | PASS | source balance = 49638808.090000026, mart total_balance = 49638808.08999998, diff = 0.00 |
| `delinquency_bucket_parity` | PASS | all buckets match SAS mapping |

**7 passed, 0 failed, 0 skipped**
