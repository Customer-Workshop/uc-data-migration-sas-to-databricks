/*
  Business rule: net_profit must equal total_revenue - operating_cost - total_ecl.
  Mirrors SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - coalesce(TOTAL_ECL, 0).
  Allows a small tolerance for floating-point arithmetic.
*/
select *
from {{ ref('mart_customer_pnl') }}
where abs(net_profit - (total_revenue - operating_cost - total_ecl)) > 0.01
