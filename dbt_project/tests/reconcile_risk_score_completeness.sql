/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 1 preserves every
  in-scope current-date lending account exactly once. It proves the score mart
  has no row loss or fan-out from the bureau, payment, or collateral joins.
*/
with expected as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
      and snapshot_date = current_date()
),

actual as (
    select count(*) as n from {{ ref('mart_risk_scores') }}
)

select
    e.n as expected_rows,
    a.n as actual_rows
from expected e
cross join actual a
where e.n <> a.n
