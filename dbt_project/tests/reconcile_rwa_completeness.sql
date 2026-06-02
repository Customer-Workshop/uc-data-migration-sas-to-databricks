/*
  Reconciliation test: RWA completeness against the documented in-scope population.

  monthly_regulatory_reporting.sas (Step 1) builds MONTHLY_RWA from the month-end
  account snapshot with no account-type filter (only SNAPSHOT_DATE = month_end).
  The migrated in-scope population is therefore the full int_account_metrics
  snapshot (which already applies the staging scope filter status not in 'W','C').

  This control proves the conversion neither dropped rows silently nor fanned out
  on the collateral join: the sum of N_ACCOUNTS across all RWA groups must equal
  the in-scope account count.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n from {{ ref('int_account_metrics') }}
),

model_accounts as (
    select coalesce(sum(n_accounts), 0) as n from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_in_scope_accounts,
    m.n as model_accounts,
    m.n - e.n as difference
from expected_in_scope e
cross join model_accounts m
where e.n <> m.n
