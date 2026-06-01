/*
  Reconciliation: delinquency aging control total.

  Sum of total_balance in the mart must equal sum of current_balance from the
  source (int_account_metrics) for credit product types only.
*/
with source_total as (
    select coalesce(sum(current_balance), 0) as total
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_total as (
    select coalesce(sum(total_balance), 0) as total
    from {{ ref('mart_delinquency_aging') }}
)

select
    s.total as expected_total_balance,
    m.total as mart_total_balance,
    m.total - s.total as difference
from source_total s
cross join mart_total m
where abs(m.total - s.total) > 0.01
