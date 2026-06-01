/*
  Reconciliation control: RWA completeness (no silent row loss / no fan-out).

  monthly_regulatory_reporting.sas Step 1 aggregates the ENTIRE daily account
  snapshot (STG_BANK.CUST_ACCOUNTS_DAILY) — there is no account-type filter on
  the RWA query. The dbt mart must therefore cover exactly the same population
  as int_account_metrics (the dbt daily snapshot). The LEFT JOIN to loan_details
  is on a unique key, so it must not multiply rows either.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with source_count as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
),

mart_count as (
    select coalesce(sum(n_accounts), 0) as n
    from {{ ref('mart_regulatory_rwa') }}
)

select
    s.n as expected_accounts,
    m.n as mart_accounts,
    m.n - s.n as difference
from source_count s
cross join mart_count m
where s.n <> m.n
