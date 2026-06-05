/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL computing Risk-Weighted Assets by category using Basel III
    standardized approach risk weights. Joins STG_BANK.CUST_ACCOUNTS_DAILY
    to ORA_DW.LOAN_DETAILS on ACCOUNT_ID.

  dbt Equivalent:
    SQL CASE replaces SAS CASE. The SAS ORA_DW.LOAN_DETAILS is a composite
    view; in Databricks the LTV data comes from the collateral table
    (LTV = current_balance / collateral_value). ref('int_account_metrics')
    replaces STG_BANK.CUST_ACCOUNTS_DAILY. var('prev_ym') replaces &PREV_YM.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select * from {{ source('banking_raw', 'collateral') }}
),

weighted as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG'
                 and c.collateral_value > 0
                 and (a.current_balance / c.collateral_value) <= 0.80
                then 0.35
            when a.account_type = 'MTG' then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            -- Source-faithful: LOC explicitly mapped to 1.00 in SAS source
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from accounts a
    left join collateral c
        on a.account_id = c.account_id
),

aggregated as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        customer_segment,
        risk_weight,
        count(*) as n_accounts,
        sum(current_balance) as total_exposure,
        sum(current_balance * risk_weight) as rwa
    from weighted
    group by 1, 2, 3, 4
)

select * from aggregated
