/*
  Business rule: total_revenue must equal net_interest_income + fee_income.
  Mirrors SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0).
  Allows a small tolerance for floating-point arithmetic.
*/
select *
from {{ ref('mart_customer_pnl') }}
where abs(total_revenue - (net_interest_income + fee_income)) > 0.01
