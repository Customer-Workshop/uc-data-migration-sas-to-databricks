/*
  Reconciliation test: REPORTS.MONTHLY_RWA control totals.
  Program: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  Two totals must tie out against the raw source:
    * TOTAL_EXPOSURE — sum(CURRENT_BALANCE) over the in-scope population;
    * RWA — sum(CURRENT_BALANCE * RISK_WEIGHT), recomputed here from the SAS
      mapping (see reconcile_rwa_risk_weight_parity.sql for the branch-level
      check) rather than read back out of the mart.

  MONTHLY_RWA.SUM_RWA is also one of the SAS golden control totals
  (verify/fixtures/sas_golden/controls.csv), compared in verify/reconcile.py.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_population as (
    select
        a.account_id,
        a.account_type,
        a.current_balance,
        l.ltv
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= {{ sas_run_date() }}
),

sas_mapping as (
    select *
    from (
        values
        ('CHK', 'any', 0.00),
        ('SAV', 'any', 0.00),
        ('MMA', 'any', 0.00),
        ('CD', 'any', 0.00),
        ('MTG', 'LTV<=0.80', 0.35),
        ('MTG', 'LTV>0.80', 0.50),
        ('HELC', 'any', 0.50),
        ('AUTO', 'any', 0.75),
        ('PERS', 'any', 0.75),
        ('CC', 'any', 0.75),
        ('LOC', 'any', 1.00)
    ) as m (account_type, ltv_band, expected_risk_weight)
),

source_weighted as (
    select
        s.current_balance,
        -- SAS catch-all `else 1.00` for any account type without a branch.
        coalesce(m.expected_risk_weight, 1.00) as risk_weight
    from source_population s
    left join sas_mapping m
        on s.account_type = m.account_type
        and (
            m.ltv_band = 'any'
            -- SAS missing numerics sort below every number, so a missing LTV
            -- satisfies `LTV <= 0.80`.
            or (m.ltv_band = 'LTV<=0.80' and (s.ltv is null or s.ltv <= 0.80))
            or (m.ltv_band = 'LTV>0.80' and s.ltv > 0.80)
        )
),

expected as (
    select
        round(sum(current_balance), 2) as exposure,
        round(sum(current_balance * risk_weight), 2) as rwa
    from source_weighted
),

reported as (
    select
        round(sum(total_exposure), 2) as exposure,
        round(sum(rwa), 2) as rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    'total_exposure' as control,
    e.exposure as sas_source_value,
    r.exposure as mart_value
from expected e
cross join reported r
where abs(e.exposure - r.exposure) > 0.01

union all

select
    'sum_rwa' as control,
    e.rwa as sas_source_value,
    r.rwa as mart_value
from expected e
cross join reported r
where abs(e.rwa - r.rwa) > 0.01
