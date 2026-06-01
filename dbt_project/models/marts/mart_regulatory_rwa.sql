/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1: REPORTS.MONTHLY_RWA)

  SAS Original:
    PROC SQL computing Basel III standardized-approach Risk-Weighted Assets by
    ACCOUNT_TYPE / CUSTOMER_SEGMENT, with an LTV-dependent risk weight for
    mortgages (calculated column referenced within the same query).

  dbt Equivalent:
    PROC SQL aggregation becomes GROUP BY; the LTV-dependent CASE is computed in
    a CTE so it can be referenced by both the risk_weight and the rwa columns
    (Spark SQL has no "calculated" keyword like SAS PROC SQL).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select
        account_id,
        collateral_value
    from {{ source('banking_raw', 'collateral') }}
),

with_ltv as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when c.collateral_value > 0
                then a.current_balance / c.collateral_value
            else null
        end as ltv
    from accounts a
    left join collateral c on a.account_id = c.account_id
),

weighted as (
    select
        date_format(current_date(), 'yyyyMM') as report_month,
        account_type,
        customer_segment,
        current_balance,
        -- Basel III standardized risk weights — mirrors the CASE in the SAS
        -- source (monthly_regulatory_reporting.sas) account-type for
        -- account-type, including LOC -> 1.00 and the else -> 1.00 catch-all
        -- (IRA is not enumerated in the source and so lands on else, a legacy
        -- quirk reproduced here for source parity, not silently "corrected").
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD' then 0.00
            when account_type = 'MTG' and coalesce(ltv, 1) <= 0.80 then 0.35
            when account_type = 'MTG' and coalesce(ltv, 1) > 0.80 then 0.50
            when account_type = 'HELC' then 0.50
            when account_type in ('AUTO', 'PERS') then 0.75
            when account_type = 'CC' then 0.75
            when account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from with_ltv
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from weighted
group by report_month, account_type, customer_segment, risk_weight
