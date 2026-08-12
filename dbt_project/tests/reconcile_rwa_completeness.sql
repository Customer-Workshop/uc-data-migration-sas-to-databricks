/*
  Reconciliation: RWA completeness.

  monthly_regulatory_reporting.sas Step 1 aggregates EVERY account in the
  month-end snapshot (no type filter; left join to LOAN_DETAILS cannot drop
  rows and, at one loan per account, cannot fan out). Therefore
  sum(n_accounts) in the mart must equal the snapshot population.

  dbt singular test convention: FAILS if this query returns rows.
*/
with expected as (
    select count(*) as n from {{ ref('int_account_metrics') }}
),

actual as (
    select sum(n_accounts) as n from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_accounts,
    a.n as mart_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
