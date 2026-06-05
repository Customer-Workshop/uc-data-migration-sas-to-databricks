/*
  Reconciliation test: delinquency aging completeness.

  mart_delinquency_aging must cover all in-scope loan accounts — those with
  account_type in (MTG, AUTO, PERS, CC, LOC, HELC) from int_account_metrics.
  The sum of n_accounts in the mart must equal the count of in-scope accounts
  from the intermediate model.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

actual as (
    select sum(n_accounts) as n from {{ ref('mart_delinquency_aging') }}
)

select
    e.n as expected_accounts,
    a.n as actual_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
