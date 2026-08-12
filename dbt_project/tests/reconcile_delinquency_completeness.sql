/*
  Reconciliation: delinquency aging completeness.

  monthly_regulatory_reporting.sas Step 2 scopes to lending products
  ('MTG','AUTO','PERS','CC','LOC','HELC') and left joins LOAN_DETAILS
  (no row loss, no fan-out at one loan per account). sum(n_accounts) in
  the mart must equal that in-scope population exactly.

  dbt singular test convention: FAILS if this query returns rows.
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
    a.n as mart_accounts,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
