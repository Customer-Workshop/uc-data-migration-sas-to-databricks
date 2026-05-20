/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL computing Risk-Weighted Assets by account type and
    customer segment using Basel III standardized approach risk weights.
    Joins STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS.

  dbt Equivalent:
    SQL CASE expression for risk weight assignment.
    SQL GROUP BY replaces PROC SQL aggregation.
    dbt var('prev_ym') replaces SAS &PREV_YM macro variable.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loans as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

-- SAS: Basel III standardized approach risk weights
rwa_detail as (
    select
        {{ var('prev_ym') }} as report_month,
        a.account_type,
        a.customer_segment,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG'
                 and coalesce(l.ltv, 1) <= 0.80           then 0.35
            when a.account_type = 'MTG'
                 and coalesce(l.ltv, 1) > 0.80            then 0.50
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight,
        a.current_balance
    from accounts a
    left join loans l
        on a.account_id = l.account_id
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*)                              as n_accounts,
    sum(current_balance)                  as total_exposure,
    sum(current_balance * risk_weight)    as rwa
from rwa_detail
group by
    report_month,
    account_type,
    customer_segment,
    risk_weight
order by account_type, customer_segment
