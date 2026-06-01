/*
  Reconciliation: RWA completeness.

  The SAS Step 1 (PROC SQL) reads every account from STG_BANK.CUST_ACCOUNTS_DAILY
  and LEFT JOINs to ORA_DW.LOAN_DETAILS. The LEFT JOIN guarantees no row loss —
  every account receives a risk weight. The dbt mart must therefore contain
  exactly the same number of accounts as its source (int_account_metrics).

  Fails if sum(n_accounts) in the mart differs from count(*) in the source.
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
    s.n as source_accounts,
    m.n as mart_accounts,
    m.n - s.n as difference
from source_count s
cross join mart_count m
where s.n <> m.n
