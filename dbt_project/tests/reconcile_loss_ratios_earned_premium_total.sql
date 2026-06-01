/*
  Reconciliation test: earned premium control total.

  The mart (mart_loss_ratios) aggregates YTD_EARNED_PREMIUM from
  int_policy_valuation by POLICY_TYPE. The sum of TOTAL_EARNED in the mart
  must equal the sum of YTD_EARNED_PREMIUM in the intermediate model —
  proving the GROUP BY neither lost rows nor double-counted.

  Tolerance: 0.01 (rounding from double aggregation).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with intermediate_total as (
    select
        coalesce(sum(ytd_earned_premium), 0) as total
    from {{ ref('int_policy_valuation') }}
),

mart_total as (
    select
        coalesce(sum(total_earned), 0) as total
    from {{ ref('mart_loss_ratios') }}
)

select
    i.total as intermediate_earned_premium,
    m.total as mart_earned_premium,
    abs(i.total - m.total) as difference
from intermediate_total i
cross join mart_total m
where abs(i.total - m.total) > 0.01
