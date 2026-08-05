/*
  Reconciliation test: delinquency aging preserves exactly the SAS
  in-scope account types (MTG, AUTO, PERS, CC, LOC, HELC).
  dbt singular tests fail when this query returns any rows.
*/
with expected as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

actual as (
    select sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
)

select
    e.n as expected_accounts,
    a.n as model_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
