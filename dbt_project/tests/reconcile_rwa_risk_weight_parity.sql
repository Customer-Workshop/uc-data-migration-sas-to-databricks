/*
  Reconciliation: risk-weight parity — every account-type/LTV combination
  must map to the SAS-defined risk weight, value-for-value.

  This is the most important control for this program. A mapping can produce
  a correct aggregate total while an individual branch is wrong (e.g. LOC
  mapped to 0.75 instead of 1.00 would still sum correctly if LOC volume
  happens to offset). This check catches that.

  The expected mapping (from monthly_regulatory_reporting.sas, Step 1):
    CHK/SAV/MMA      -> 0.00
    CD               -> 0.00
    MTG (LTV <= 0.80)-> 0.35
    MTG (LTV > 0.80) -> 0.50
    HELC             -> 0.50
    AUTO/PERS        -> 0.75
    CC               -> 0.75
    LOC              -> 1.00   (source-faithful, not 0.75)
    else (IRA, etc.) -> 1.00

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart_weights as (
    select distinct
        account_type,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
),

expected as (
    select 'CHK' as account_type, 0.00 as expected_weight
    union all select 'SAV',  0.00
    union all select 'MMA',  0.00
    union all select 'CD',   0.00
    union all select 'HELC', 0.50
    union all select 'AUTO', 0.75
    union all select 'PERS', 0.75
    union all select 'CC',   0.75
    union all select 'LOC',  1.00
    union all select 'IRA',  1.00
)

select
    coalesce(m.account_type, e.account_type) as account_type,
    m.risk_weight as actual_weight,
    e.expected_weight,
    'MISMATCH' as status
from expected e
left join mart_weights m
    on e.account_type = m.account_type
    and abs(e.expected_weight - m.risk_weight) < 0.001
where m.account_type is null
