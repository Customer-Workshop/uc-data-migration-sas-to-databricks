/*
  Reconciliation: RWA control total.

  Sum of total_exposure in the mart must equal sum of current_balance from the
  source (int_account_metrics).  A mismatch means the aggregation lost or
  duplicated balance.
*/
with source_total as (
    select coalesce(sum(current_balance), 0) as total
    from {{ ref('int_account_metrics') }}
),

mart_total as (
    select coalesce(sum(total_exposure), 0) as total
    from {{ ref('mart_regulatory_rwa') }}
)

select
    s.total as expected_total_exposure,
    m.total as mart_total_exposure,
    m.total - s.total as difference
from source_total s
cross join mart_total m
where abs(m.total - s.total) > 0.01
