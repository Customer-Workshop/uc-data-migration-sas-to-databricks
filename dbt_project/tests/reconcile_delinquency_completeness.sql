/*
  Reconciliation test: delinquency aging covers all lending accounts.

  The SAS report (monthly_regulatory_reporting.sas, Step 2) buckets every
  lending account (MTG, AUTO, PERS, CC, LOC, HELC). The dbt mart
  (mart_delinquency_aging) must account for the same population — no
  silent row loss or fan-out.

  Fails if this query returns any rows.
*/
with source_count as (
    select count(*) as n
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

model_count as (
    select sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
)

select
    s.n as source_lending_accounts,
    m.n as model_bucketed_accounts,
    m.n - s.n as difference
from source_count s
cross join model_count m
where s.n <> m.n
