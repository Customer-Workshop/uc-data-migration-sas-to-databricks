/*
  Reconciliation: delinquency aging control total.

  Total balance in the mart must tie out to the sum of CURRENT_BALANCE for
  the in-scope lending population — the SAS SUM(CURRENT_BALANCE) control
  total for Step 2.

  dbt singular test convention: FAILS if this query returns rows.
*/
with expected as (
    select sum(current_balance) as total
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

actual as (
    select sum(total_balance) as total from {{ ref('mart_delinquency_aging') }}
)

select
    e.total as expected_total_balance,
    a.total as mart_total_balance,
    a.total - e.total as difference
from expected e
cross join actual a
where abs(a.total - e.total) > 0.01
