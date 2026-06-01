/*
  Reconciliation: delinquency balance control total.

  Total balance across all delinquency buckets must equal the sum of
  current_balance for lending accounts in the source. Any discrepancy
  means the WHERE filter or GROUP BY introduced silent row loss or fan-out.

  Tolerance: 0.01 (penny rounding).
*/
with source_total as (
    select sum(current_balance) as total_balance
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_total as (
    select coalesce(sum(total_balance), 0) as total_balance
    from {{ ref('mart_delinquency_aging') }}
)

select
    s.total_balance as source_total_balance,
    m.total_balance as mart_total_balance,
    abs(m.total_balance - s.total_balance) as difference
from source_total s
cross join mart_total m
where abs(m.total_balance - s.total_balance) > 0.01
