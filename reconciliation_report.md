# Reconciliation Report — banking_analytics / namespace `child1`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 468, model accounts = 468 |
| `rwa_completeness` | PASS | source accounts = 468, mart accounts = 468 |
| `rwa_control_total` | PASS | exposure src=94961804.93999995 mart=94961804.94 (diff 0.00); rwa src=43090822.89299996 mart=43090822.893000014 (diff 0.00) |
| `rwa_risk_weight_parity` | PASS | all account_type risk weights match the SAS mapping |
| `delinquency_completeness` | PASS | credit-product accounts = 249, mart accounts = 249 |
| `delinquency_control_total` | PASS | balance src=49638808.090000026 mart=49638808.089999996 (diff 0.00); past_due src=0.0 mart=0.0 (diff 0.00) |
| `delinquency_bucket_parity` | PASS | all (type, region, bucket) counts match the SAS mapping |

**7 passed, 0 failed, 0 skipped**
