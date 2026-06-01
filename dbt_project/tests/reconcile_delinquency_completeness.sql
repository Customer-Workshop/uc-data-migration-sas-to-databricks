/*
  Reconciliation: delinquency aging completeness.

  The SAS Step 2 (monthly_regulatory_reporting.sas) filters to credit product
  types: MTG, AUTO, PERS, CC, LOC, HELC.  The dbt mart should contain exactly
  the same population as int_account_metrics filtered to those types.
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
    s.n as expected_accounts,
    m.n as mart_accounts,
    m.n - s.n as difference
from source_count s
cross join mart_count m
where s.n <> m.n
