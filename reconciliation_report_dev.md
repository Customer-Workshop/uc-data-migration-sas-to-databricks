# Reconciliation Report — banking_analytics / namespace `dev`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 468, model accounts = 468 |
| `rwa_completeness` | PASS | snapshot accounts = 468, mart accounts = 468 |
| `rwa_control_total` | PASS | snapshot balance = 94,961,804.94, mart exposure = 94,961,804.94 |
| `rwa_risk_weight_parity` | PASS | every account-type risk weight matches the SAS mapping |
| `delinquency_completeness` | PASS | in-scope lending accounts = 249, mart accounts = 249 |
| `delinquency_bucket_parity` | PASS | every aging bucket count matches the SAS bands |

**6 passed, 0 failed, 0 skipped**
