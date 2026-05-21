/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1, lines 40-67)

  SAS Original:
    PROC SQL joining STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS,
    applying Basel III standardized risk weights via CASE,
    aggregating by report_month / account_type / customer_segment / risk_weight

  dbt Equivalent:
    SQL CASE expression replaces SAS CASE, var('report_month') replaces &report_month,
    ref() / source() replace SAS LIBNAMEs,
    last_day() filter replaces SAS "&month_end"d snapshot logic
*/

{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

with accounts as (
    -- SAS: STG_BANK.CUST_ACCOUNTS_DAILY → dbt: ref('stg_cust_accounts')
    select * from {{ ref('stg_cust_accounts') }}
),

loan_details as (
    -- SAS: ORA_DW.LOAN_DETAILS → dbt: source('banking_raw', 'loan_details')
    select * from {{ source('banking_raw', 'loan_details') }}
),

-- SAS: PROC SQL CREATE TABLE REPORTS.MONTHLY_RWA → dbt: SELECT with GROUP BY
rwa as (
    select
        {{ var('report_month') }}                   as report_month,
        a.account_type,
        a.customer_segment,

        -- SAS: Basel III standardized risk weight CASE expression
        case
            when a.account_type in ('CHK', 'SAV', 'MMA')           then 0.00
            when a.account_type = 'CD'                              then 0.00
            when a.account_type = 'MTG' and l.ltv <= 0.80           then 0.35
            when a.account_type = 'MTG' and l.ltv >  0.80           then 0.50
            when a.account_type = 'HELC'                            then 0.50
            when a.account_type in ('AUTO', 'PERS')                 then 0.75
            when a.account_type = 'CC'                              then 0.75
            when a.account_type = 'LOC'                             then 1.00
            else 1.00
        end                                                         as risk_weight,

        count(*)                                                    as n_accounts,
        sum(a.current_balance)                                      as total_exposure,

        -- SAS: sum(CURRENT_BALANCE * calculated RISK_WEIGHT) → repeated CASE for RWA
        sum(
            a.current_balance * case
                when a.account_type in ('CHK', 'SAV', 'MMA')       then 0.00
                when a.account_type = 'CD'                          then 0.00
                when a.account_type = 'MTG' and l.ltv <= 0.80       then 0.35
                when a.account_type = 'MTG' and l.ltv >  0.80       then 0.50
                when a.account_type = 'HELC'                        then 0.50
                when a.account_type in ('AUTO', 'PERS')             then 0.75
                when a.account_type = 'CC'                          then 0.75
                when a.account_type = 'LOC'                         then 1.00
                else 1.00
            end
        )                                                           as rwa

    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
    -- SAS: WHERE a.SNAPSHOT_DATE = "&month_end"d
    -- Filter to month-end snapshot using last_day of the report_month
    where a.last_activity_date <= last_day(to_date({{ var('report_month') }} || '01', 'yyyyMMdd'))
    group by 1, 2, 3, 4
)

select * from rwa
order by account_type, customer_segment
