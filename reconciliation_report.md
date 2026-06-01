# Reconciliation Report — banking_analytics / namespace `child1`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 469, model accounts = 469 |
| `rwa_completeness` | PASS | source accounts = 469, mart accounts = 469 |
| `rwa_control_total` | PASS | exposure src=93223155.41999999 mart=93223155.41999996 (diff 0.00); rwa src=42185916.75649996 mart=42185916.75649999 (diff 0.00) |
| `rwa_risk_weight_parity` | PASS | all account_type risk weights match the SAS mapping |
| `delinquency_completeness` | PASS | credit-product accounts = 252, mart accounts = 252 |
| `delinquency_control_total` | PASS | balance src=49607451.61999999 mart=49607451.61999999 (diff 0.00); past_due src=614537.77 mart=614537.7699999999 (diff 0.00) |
| `delinquency_bucket_parity` | PASS | all (type, region, bucket) counts match the SAS mapping |

**7 passed, 0 failed, 0 skipped**
