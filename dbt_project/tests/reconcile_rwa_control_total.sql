/*
  Reconciliation: RWA control total.

  Total exposure in the mart must tie out to the sum of CURRENT_BALANCE in
  the snapshot — the SAS SUM(CURRENT_BALANCE) control total. A cent of
  tolerance absorbs float aggregation ordering differences only.

  dbt singular test convention: FAILS if this query returns rows.
*/
with expected as (
    select sum(current_balance) as total from {{ ref('int_account_metrics') }}
),

actual as (
    select sum(total_exposure) as total from {{ ref('mart_regulatory_rwa') }}
)

select
    e.total as expected_total_exposure,
    a.total as mart_total_exposure,
    a.total - e.total as difference
from expected e
cross join actual a
where abs(a.total - e.total) > 0.01
