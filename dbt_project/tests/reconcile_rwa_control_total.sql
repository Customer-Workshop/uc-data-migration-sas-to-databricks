/*
  Reconciliation test: RWA control totals tie out to source.

  Independently re-derives, at account grain from int_account_metrics (+ collateral
  for LTV), the Basel III risk weight using the exact SAS CASE from
  monthly_regulatory_reporting.sas (Step 1), then compares the source control
  totals — total exposure (sum of current_balance) and total RWA
  (sum of current_balance * risk_weight) — against the mart's aggregated totals.

  A divergence here means the conversion changed the exposure population or the
  risk-weight assignment. Investigate against the SAS source; do not relax the
  control. Amounts are rounded to cents to avoid floating-point noise.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_accounts as (
    select
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG'
                and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) <= 0.80
                then 0.35
            when a.account_type = 'MTG'
                and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) > 0.80
                then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected as (
    select
        round(sum(current_balance), 2) as exposure,
        round(sum(current_balance * risk_weight), 2) as rwa
    from source_accounts
),

actual as (
    select
        round(sum(total_exposure), 2) as exposure,
        round(sum(rwa), 2) as rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    e.exposure as expected_exposure,
    a.exposure as actual_exposure,
    e.rwa as expected_rwa,
    a.rwa as actual_rwa
from expected e
cross join actual a
where e.exposure <> a.exposure or e.rwa <> a.rwa
