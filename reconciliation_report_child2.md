# Reconciliation Report — banking_analytics / namespace `child2`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | PASS | in-scope raw accounts = 468, model accounts = 468 |
| `claims_completeness` | PASS | in-scope valid claims = 82, model claims = 82 |
| `claims_control_total` | PASS | claimed src=21842494.97 model=21842494.97; approved src=23725.09 model=23725.09 |
| `claims_fraud_parity` | PASS | per-claim fraud_risk mismatches vs source = 0 |
| `claims_adjudication_parity` | PASS | per-claim adjudication mismatches vs source = 0 |
| `claims_curated_routing` | PASS | register=82 (model 82), review_queue=81 (expected 81), fraud_alerts=12 (expected 12), orphan rows=0 |

**6 passed, 0 failed, 0 skipped**
