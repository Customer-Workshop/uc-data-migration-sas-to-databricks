/*
  Reconciliation test: RWA completeness.

  The mart_regulatory_rwa model must account for every row in
  int_account_metrics (one account = one row in the weighted CTE before
  aggregation). The sum of n_accounts in the mart must equal the total
  account count from the intermediate model — proving no silent row loss
  or fan-out during the join + group-by.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
),

actual as (
    select sum(n_accounts) as n from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_accounts,
    a.n as actual_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
