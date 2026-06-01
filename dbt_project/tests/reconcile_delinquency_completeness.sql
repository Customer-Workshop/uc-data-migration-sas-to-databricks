/*
  Reconciliation: delinquency aging completeness.

  The SAS Step 2 selects all lending accounts
  (MTG, AUTO, PERS, CC, LOC, HELC) from STG_BANK.CUST_ACCOUNTS_DAILY.
  The dbt mart must contain the same count of lending accounts as its
  source (int_account_metrics filtered to lending types).

  Fails if sum(n_accounts) in the mart differs from source lending count.
*/
with source_count as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_count as (
    select coalesce(sum(n_accounts), 0) as n
    from {{ ref('mart_delinquency_aging') }}
)

select
    s.n as source_lending_accounts,
    m.n as mart_accounts,
    m.n - s.n as difference
from source_count s
cross join mart_count m
where s.n <> m.n
