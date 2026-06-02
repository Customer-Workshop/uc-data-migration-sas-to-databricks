/*
  Reconciliation test: delinquency aging completeness against the in-scope population.

  monthly_regulatory_reporting.sas (Step 2) restricts DELINQUENCY_AGING to lending
  products ('MTG','AUTO','PERS','CC','LOC','HELC') of the month-end snapshot. The
  migrated in-scope population is therefore int_account_metrics filtered to those
  account types. The sum of N_ACCOUNTS across all aging groups must equal that
  in-scope count (no silent row loss, no fan-out on the payment_history join).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

model_accounts as (
    select coalesce(sum(n_accounts), 0) as n from {{ ref('mart_delinquency_aging') }}
)

select
    e.n as expected_in_scope_accounts,
    m.n as model_accounts,
    m.n - e.n as difference
from expected_in_scope e
cross join model_accounts m
where e.n <> m.n
