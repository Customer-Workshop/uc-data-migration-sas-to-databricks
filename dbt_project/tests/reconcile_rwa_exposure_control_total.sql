/*
  Reconciliation: RWA exposure control total.

  Total exposure in the mart must equal the sum of current_balance from the
  source (int_account_metrics). Any discrepancy means the GROUP BY or JOIN
  introduced silent row loss or fan-out.

  Tolerance: 0.01 (penny rounding).
*/
with source_total as (
    select sum(current_balance) as total_balance
    from {{ ref('int_account_metrics') }}
),

mart_total as (
    select coalesce(sum(total_exposure), 0) as total_exposure
    from {{ ref('mart_regulatory_rwa') }}
)

select
    s.total_balance as source_total_balance,
    m.total_exposure as mart_total_exposure,
    abs(m.total_exposure - s.total_balance) as difference
from source_total s
cross join mart_total m
where abs(m.total_exposure - s.total_balance) > 0.01
