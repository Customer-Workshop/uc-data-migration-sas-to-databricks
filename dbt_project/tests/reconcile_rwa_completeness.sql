/*
  Reconciliation: RWA completeness — no silent row loss vs the in-scope
  source population.

  The SAS Step 1 (REPORTS.MONTHLY_RWA) processes ALL accounts from
  STG_BANK.CUST_ACCOUNTS_DAILY (left join to LOAN_DETAILS, no account-type
  filter). The dbt equivalent reads from int_account_metrics.

  This test verifies that sum(n_accounts) in mart_regulatory_rwa equals the
  total in-scope accounts from int_account_metrics. A mismatch signals either
  silent row loss or an unintended fan-out from the collateral join.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with model_total as (
    select coalesce(sum(n_accounts), 0) as n
    from {{ ref('mart_regulatory_rwa') }}
),

source_total as (
    select count(*) as n from {{ ref('int_account_metrics') }}
)

select
    s.n as expected_accounts,
    m.n as model_accounts,
    m.n - s.n as difference
from source_total s
cross join model_total m
where s.n <> m.n
