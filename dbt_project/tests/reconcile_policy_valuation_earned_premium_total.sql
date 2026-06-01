/*
  Reconciliation test: earned premium control total.

  The mart_loss_ratios total_earned (sum across all policy types) must equal
  the sum of ytd_earned_premium from int_policy_valuation. This catches
  aggregation errors, double-counting, or dropped rows between the
  intermediate and mart layers.

  dbt singular test convention: the test FAILS if this query returns any rows.
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
    i.total_earned as intermediate_earned,
    m.total_earned as mart_earned,
    abs(i.total_earned - m.total_earned) as difference
from intermediate_total i
cross join mart_total m
where abs(i.total_earned - m.total_earned) > 0.01
