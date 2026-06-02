/*
  Reconciliation control: earned-premium control total.

  Total YTD earned premium must tie out between the intermediate layer
  (int_policy_valuation, per policy) and the mart layer (mart_loss_ratios,
  aggregated by policy type). PROC MEANS in the SAS source is a pure
  re-aggregation of the same population, so the grand totals must be equal.
  Any difference indicates a fan-out, a filter mismatch, or an aggregation
  error introduced in the mart.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with intermediate_total as (
    select coalesce(sum(ytd_earned_premium), 0) as total_earned
    from {{ ref('int_policy_valuation') }}
),

mart_total as (
    select coalesce(sum(total_earned), 0) as total_earned
    from {{ ref('mart_loss_ratios') }}
)

select
    i.total_earned as int_total_earned,
    m.total_earned as mart_total_earned,
    abs(i.total_earned - m.total_earned) as difference
from intermediate_total i
cross join mart_total m
where abs(i.total_earned - m.total_earned) > 0.01
