/*
  Reconciliation test: RWA exposure control total.

  The total exposure reported by mart_regulatory_rwa (sum of total_exposure across
  all risk-weight buckets) must tie out to the sum of account balances feeding it
  from int_account_metrics. This is the classic "control total" reconciliation a
  SAS analyst would run by hand (PROC SQL SELECT SUM(...)) to prove a GROUP BY did
  not lose or double-count exposure. Here it runs automatically on every build.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_balance as (
    select sum(current_balance) as bal from {{ ref('int_account_metrics') }}
),

mart_exposure as (
    select sum(total_exposure) as bal from {{ ref('mart_regulatory_rwa') }}
)

select
    s.bal as source_balance,
    m.bal as mart_exposure,
    abs(coalesce(s.bal, 0) - coalesce(m.bal, 0)) as difference
from source_balance s
cross join mart_exposure m
where abs(coalesce(s.bal, 0) - coalesce(m.bal, 0)) > 0.01
