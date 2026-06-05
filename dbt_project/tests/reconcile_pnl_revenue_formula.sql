/*
  Reconciliation test: total_revenue formula parity.

  SAS customer_profitability.sas (Step 4):
    TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0);
  The SAS sum() function treats missing values as 0.

  dbt equivalent:
    total_revenue = coalesce(net_interest_income, 0) + coalesce(fee_income, 0)

  This control verifies per-row: total_revenue equals the sum of its components
  with missing treated as zero — no row may diverge.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select
    customer_id,
    net_interest_income,
    fee_income,
    total_revenue,
    coalesce(net_interest_income, 0) + coalesce(fee_income, 0)
        as expected_revenue
from {{ ref('mart_customer_pnl') }}
where abs(
    total_revenue
    - (coalesce(net_interest_income, 0) + coalesce(fee_income, 0))
) > 0.01
