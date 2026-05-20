/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL aggregating Risk-Weighted Assets by account type and
    customer segment using Basel III standardized approach risk weights.
    Joined STG_BANK.CUST_ACCOUNTS_DAILY with ORA_DW.LOAN_DETAILS
    for LTV on secured lending.

  dbt Equivalent:
    SQL CASE replaces SAS calculated-column risk weight assignment
    ref('int_account_metrics') replaces STG_BANK.CUST_ACCOUNTS_DAILY
    source('banking_raw', 'loan_details') replaces ORA_DW.LOAN_DETAILS
    GROUP BY replaces PROC SQL aggregation
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select * from {{ source('banking_raw', 'collateral') }}
),

-- Compute LTV for secured accounts (same pattern as mart_risk_scores)
account_with_ltv as (
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
    left join collateral c
        on a.account_id = c.account_id
),

-- SAS: Basel III risk weight assignment
risk_weighted as (
    select
        {{ var('prev_ym') }} as report_month,
        account_type,
        customer_segment,
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD'                    then 0.00
            when account_type = 'MTG' and ltv <= 0.80   then 0.35
            when account_type = 'MTG' and ltv > 0.80    then 0.50
            when account_type = 'HELC'                  then 0.50
            when account_type in ('AUTO', 'PERS')       then 0.75
            when account_type = 'CC'                    then 0.75
            when account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight,
        count(*) as n_accounts,
        sum(current_balance) as total_exposure,
        sum(current_balance * case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD'                    then 0.00
            when account_type = 'MTG' and ltv <= 0.80   then 0.35
            when account_type = 'MTG' and ltv > 0.80    then 0.50
            when account_type = 'HELC'                  then 0.50
            when account_type in ('AUTO', 'PERS')       then 0.75
            when account_type = 'CC'                    then 0.75
            when account_type = 'LOC'                   then 1.00
            else 1.00
        end) as rwa
    from account_with_ltv
    group by 1, 2, 3, 4
)

select * from risk_weighted
