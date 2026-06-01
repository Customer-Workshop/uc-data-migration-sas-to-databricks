/*
  Reconciliation: RWA completeness.

  The SAS Step 1 (monthly_regulatory_reporting.sas) aggregates ALL accounts
  from STG_BANK.CUST_ACCOUNTS_DAILY (no account-type filter on the RWA query).
  The dbt mart should therefore contain exactly the same population as
  int_account_metrics (the dbt equivalent of the daily snapshot).

  Fails if the total account count in mart_regulatory_rwa differs from the
  source population.
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
