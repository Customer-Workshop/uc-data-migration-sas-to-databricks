/*
  Reconciliation test: earned premium control total.

  Total YTD earned premium must tie between int_policy_valuation and
  mart_loss_ratios.  Any difference indicates a fan-out, filter mismatch,
  or aggregation error.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with intermediate_total as (
    select
        coalesce(sum(ytd_earned_premium), 0) as total_earned
    from {{ ref('int_policy_valuation') }}
),

mart_total as (
    select
        coalesce(sum(total_earned), 0) as total_earned
    from {{ ref('mart_loss_ratios') }}
)

select
    i.total_earned as int_total_earned,
    m.total_earned as mart_total_earned,
    abs(i.total_earned - m.total_earned) as difference
from intermediate_total i
cross join mart_total m
where abs(i.total_earned - m.total_earned) > 0.01
