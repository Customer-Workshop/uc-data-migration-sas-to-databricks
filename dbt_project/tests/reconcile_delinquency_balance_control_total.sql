/*
  Reconciliation: delinquency balance control total.

  Sum of total_balance in the mart must equal the sum of current_balance for
  lending account types in the source. Proves balance data is neither dropped
  nor fanned out during the bucketing aggregation.

  dbt singular test: returns rows on divergence → fails the build.
*/
with source_total as (
    select sum(current_balance) as total_balance
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_total as (
    select sum(total_balance) as total_balance
    from {{ ref('mart_delinquency_aging') }}
)

select
    s.total_balance as expected_balance,
    m.total_balance as actual_balance,
    abs(m.total_balance - s.total_balance) as difference
from source_total s
cross join mart_total m
where abs(coalesce(m.total_balance, 0) - coalesce(s.total_balance, 0)) > 0.01
