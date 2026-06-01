# Reconciliation Report — banking_analytics / namespace `session2`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | SKIP | int_account_metrics not found |
| `claims_completeness` | PASS | expected valid claims = 83, model claims = 83 |
| `claims_control_total` | PASS | staging sum = 20473576.29, adjudication sum = 20473576.29 |
| `claims_fraud_risk_parity` | PASS | mismatched fraud_risk rows = 0 |
| `claims_adjudication_parity` | PASS | mismatched adjudication_result rows = 0 |
| `claims_register_within_source` | PASS | adjudication rows = 83, register rows = 83 |

**5 passed, 0 failed, 1 skipped**
