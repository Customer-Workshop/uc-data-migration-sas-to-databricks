/*
  Reconciliation control: delinquency aging completeness.

  monthly_regulatory_reporting.sas Step 2 filters to credit products
  ('MTG','AUTO','PERS','CC','LOC','HELC'). The dbt mart must cover exactly that
  in-scope population from int_account_metrics — no silent row loss, no fan-out.

  dbt singular test convention: FAILS if this query returns any rows.
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
