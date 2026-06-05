/*
  Reconciliation: delinquency completeness — no silent row loss vs the
  in-scope lending population.

  The SAS Step 2 (REPORTS.DELINQUENCY_AGING) filters to lending account
  types: MTG, AUTO, PERS, CC, LOC, HELC. This test verifies that
  sum(n_accounts) in mart_delinquency_aging equals the count of those
  account types in int_account_metrics.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with model_total as (
    select coalesce(sum(n_accounts), 0) as n
    from {{ ref('mart_delinquency_aging') }}
),

source_total as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
)

select
    s.n as expected_accounts,
    m.n as model_accounts,
    m.n - s.n as difference
from source_total s
cross join model_total m
where s.n <> m.n
