/*
  Business rule: Combined ratio must equal loss ratio + 0.30 expense load.
  Mirrors the SAS DATA step: COMBINED_RATIO = LOSS_RATIO + 0.30.
  Allows a small tolerance for floating-point arithmetic.
*/
select *
from {{ ref('int_policy_valuation') }}
where loss_ratio is not null
  and combined_ratio is not null
  and abs(combined_ratio - (loss_ratio + 0.30)) > 0.001
