# Reconciliation Report — banking_analytics / namespace `apidemo1`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 468, model accounts = 468 |
| `rwa_completeness` | PASS | in-scope accounts = 468, mart sum(n_accounts) = 468 |
| `rwa_control_total` | PASS | mart sum(rwa) = 43090822.89300001, recomputed = 43090822.89299996, diff = 0.00 |
| `rwa_risk_weight_parity` | PASS | all weights match |
| `delinquency_completeness` | PASS | in-scope lending accounts = 249, mart sum(n_accounts) = 249 |
| `delinquency_bucket_parity` | PASS | all buckets valid: ['120-179', '30-59', '60-89', '90-119', 'Current'] |

**6 passed, 0 failed, 0 skipped**
