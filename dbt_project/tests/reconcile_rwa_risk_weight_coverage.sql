/*
  Reconciliation: RWA risk-weight parity.

  Independently re-derives the SAS CASE mapping from source data and compares
  the set of (account_type, risk_weight, n_accounts) with the mart. Any row
  returned means a divergence from the source logic — a different weight was
  assigned, or account counts do not match for a mapping pair.

  This is the control that caught the LOC → 0.75 divergence documented in
  docs/CONVERSION_PLAYBOOK.md. It compares the mart's per-type weights to the
  SAS mapping value for value.

  dbt singular test: returns rows on divergence → fails the build.
*/
with source_weights as (
    select
        a.account_type,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG'
                 and c.collateral_value is not null
                 and c.collateral_value > 0
                 and (a.current_balance / c.collateral_value) <= 0.80
                then 0.35
            when a.account_type = 'MTG'
                 and c.collateral_value is not null
                 and c.collateral_value > 0
                 and (a.current_balance / c.collateral_value) > 0.80
                then 0.50
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight,
        count(*) as n
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
    group by 1, 2
),

mart_weights as (
    select
        account_type,
        risk_weight,
        sum(n_accounts) as n
    from {{ ref('mart_regulatory_rwa') }}
    group by 1, 2
)

select
    coalesce(s.account_type, m.account_type) as account_type,
    s.risk_weight as expected_weight,
    m.risk_weight as actual_weight,
    s.n as expected_n,
    m.n as actual_n
from source_weights s
full outer join mart_weights m
    on s.account_type = m.account_type
    and s.risk_weight = m.risk_weight
where s.n is null or m.n is null or s.n <> m.n
