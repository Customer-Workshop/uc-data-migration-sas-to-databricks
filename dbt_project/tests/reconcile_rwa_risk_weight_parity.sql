/*
  Reconciliation test: every RWA group uses the exact SAS risk-weight CASE,
  including explicit LOC = 1.00 and missing-LTV MTG = 0.35 behavior.
  dbt singular tests fail when a model group has no matching expected group.
*/
with account_ltv as (
    select
        a.account_type,
        a.customer_segment,
        case
            when c.collateral_value > 0
            then a.current_balance / c.collateral_value
            else null
        end as ltv
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected as (
    select distinct
        account_type,
        customer_segment,
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD' then 0.00
            when account_type = 'MTG' and (ltv <= 0.80 or ltv is null) then 0.35
            when account_type = 'MTG' and ltv > 0.80 then 0.50
            when account_type = 'HELC' then 0.50
            when account_type in ('AUTO', 'PERS') then 0.75
            when account_type = 'CC' then 0.75
            when account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from account_ltv
),

actual as (
    select distinct
        account_type,
        customer_segment,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

select
    coalesce(e.account_type, a.account_type) as account_type,
    coalesce(e.customer_segment, a.customer_segment) as customer_segment,
    e.risk_weight as expected_risk_weight,
    a.risk_weight as model_risk_weight
from expected e
full outer join actual a
    on e.account_type = a.account_type
    and e.customer_segment = a.customer_segment
    and e.risk_weight = a.risk_weight
where e.account_type is null
   or a.account_type is null
