/*
  Reconciliation: risk-weight parity against the SAS mapping.

  Recomputes the Basel III risk weight for every account directly from the
  SAS CASE in monthly_regulatory_reporting.sas (the source of truth) and
  compares per-account-type weight sets against the mart. Parity is
  per-value, not aggregate: this is the control that catches, e.g., LOC
  being "reasonably" remapped to 0.75 when the source says 1.00, or an
  omitted branch silently landing in the catch-all.

  dbt singular test convention: FAILS if this query returns rows.
*/
with expected as (
    select distinct
        a.account_type,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            -- SAS: missing LTV <= 0.80 is TRUE (missing sorts low)
            when a.account_type = 'MTG' and (l.ltv <= 0.80 or l.ltv is null) then 0.35
            when a.account_type = 'MTG' and l.ltv > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
),

actual as (
    select distinct
        account_type,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

select
    coalesce(e.account_type, a.account_type) as account_type,
    e.risk_weight as expected_risk_weight,
    a.risk_weight as actual_risk_weight
from expected e
full outer join actual a
    on e.account_type = a.account_type
    and e.risk_weight = a.risk_weight
where e.account_type is null or a.account_type is null
