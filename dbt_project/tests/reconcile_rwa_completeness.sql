/*
  Reconciliation test: monthly RWA preserves the int_account_metrics
  population. The collateral lookup is permitted to enrich an account but
  must not drop or fan out rows.

  The SAS Step 1 population has no account-type filter. The migrated
  int_account_metrics relation represents the single current daily snapshot.
  dbt singular tests fail when this query returns any rows.
*/
with expected as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
),

actual as (
    select sum(n_accounts) as n
    from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_accounts,
    a.n as model_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
