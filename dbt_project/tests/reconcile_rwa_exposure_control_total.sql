/*
  Reconciliation: RWA exposure control total.

  Total exposure in the mart must equal the sum of current_balance from the
  source (int_account_metrics). This proves the model neither drops nor
  fans out balance data during the GROUP BY aggregation.

  dbt singular test: returns rows on divergence → fails the build.
*/
with source_total as (
    select sum(current_balance) as total_exposure
    from {{ ref('int_account_metrics') }}
),

mart_total as (
    select sum(total_exposure) as total_exposure
    from {{ ref('mart_regulatory_rwa') }}
)

select
    s.total_exposure as expected_exposure,
    m.total_exposure as actual_exposure,
    abs(m.total_exposure - s.total_exposure) as difference
from source_total s
cross join mart_total m
where abs(coalesce(m.total_exposure, 0) - coalesce(s.total_exposure, 0)) > 0.01
