# Reconciliation Report — banking_analytics / namespace `session3`

Source -> target controls proving the converted marts match the legacy
SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite
(e.g. the PySpark curated outputs) has not been produced yet.

| Control | Result | Detail |
|---|---|---|
| `account_completeness` | FAIL | in-scope raw accounts = 469, model accounts = 468 |
| `policy_valuation_completeness` | PASS | in-scope active policies = 76, model rows = 76 |
| `policy_valuation_earned_premium_total` | PASS | intermediate total = 319142.0324542433, mart total = 319142.0324542434, diff = 0.00 |
| `policy_valuation_premium_adequate_parity` | PASS | mismatched rows = 0 |
| `loss_ratios_parity` | PASS | mismatched policy_type rows = 0 |

**4 passed, 1 failed, 0 skipped**
