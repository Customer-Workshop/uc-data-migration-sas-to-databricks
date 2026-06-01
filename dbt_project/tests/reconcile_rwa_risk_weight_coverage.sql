/*
  Reconciliation: RWA risk-weight parity.

  Every (account_type, risk_weight) combination produced by the mart must
  be a valid entry in the SAS CASE mapping. If the dbt model diverges from
  the SAS definition — e.g. mapping LOC to 0.75 instead of 1.00 — this
  test catches it.

  Valid SAS mapping (monthly_regulatory_reporting.sas, Step 1):
    CHK, SAV, MMA -> 0.00
    CD            -> 0.00
    MTG           -> 0.35 (LTV <= 0.80) or 0.50 (LTV > 0.80)
    HELC          -> 0.50
    AUTO, PERS    -> 0.75
    CC            -> 0.75
    LOC           -> 1.00
    else (IRA)    -> 1.00
*/
with valid_mappings (account_type, risk_weight) as (
    select
        'CHK',
        0.00
    union all
    select
        'SAV',
        0.00
    union all
    select
        'MMA',
        0.00
    union all
    select
        'CD',
        0.00
    union all
    select
        'MTG',
        0.35
    union all
    select
        'MTG',
        0.50
    union all
    select
        'HELC',
        0.50
    union all
    select
        'AUTO',
        0.75
    union all
    select
        'PERS',
        0.75
    union all
    select
        'CC',
        0.75
    union all
    select
        'LOC',
        1.00
    union all
    select
        'IRA',
        1.00
),

mart_combos as (
    select distinct
        account_type,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

select
    m.account_type,
    m.risk_weight as actual_risk_weight,
    'NOT_IN_SAS_MAPPING' as issue
from mart_combos m
left join valid_mappings v
    on m.account_type = v.account_type
    and abs(m.risk_weight - v.risk_weight) < 0.001
where v.account_type is null
