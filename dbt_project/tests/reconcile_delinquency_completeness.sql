/*
  Reconciliation: delinquency completeness.

  The sum of n_accounts across all delinquency buckets must equal the count of
  lending-type accounts in the source (int_account_metrics), proving no silent
  row loss or fan-out.

  SAS scope filter: ACCOUNT_TYPE IN ('MTG','AUTO','PERS','CC','LOC','HELC')

  dbt singular test: returns rows on divergence → fails the build.
*/
with expected_count as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_count as (
    select sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
)

select
    e.n as expected_lending_accounts,
    m.n as mart_accounts,
    m.n - e.n as difference
from expected_count e
cross join mart_count m
where e.n <> m.n
