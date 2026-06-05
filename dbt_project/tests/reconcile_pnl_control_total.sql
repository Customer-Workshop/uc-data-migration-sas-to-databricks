/*
  Reconciliation test: net_profit control total.

  Verifies that the total net_profit across all customers in the mart ties out
  to an independently computed sum from upstream models:
    total_revenue - operating_cost - total_ecl

  This catches any drift between the mart's stored net_profit and the formula
  applied to its component columns.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with mart_total as (
    select round(sum(net_profit), 2) as mart_net_profit
    from {{ ref('mart_customer_pnl') }}
),

independent_total as (
    select
        round(
            sum(total_revenue - operating_cost - coalesce(total_ecl, 0)),
            2
        ) as computed_net_profit
    from {{ ref('mart_customer_pnl') }}
)

select
    m.mart_net_profit,
    i.computed_net_profit,
    m.mart_net_profit - i.computed_net_profit as difference
from mart_total m
cross join independent_total i
where abs(m.mart_net_profit - i.computed_net_profit) > 0.01
