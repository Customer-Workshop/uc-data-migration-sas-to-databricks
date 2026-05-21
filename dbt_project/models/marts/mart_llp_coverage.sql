/*
  mart_llp_coverage.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 3, lines 114-141)

  SAS Original:
    PROC SQL inner joining STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS,
    computing gross loans, total allowance, coverage %, NPL balance, NPL coverage %,
    grouped by report_month / account_type

  dbt Equivalent:
    SQL aggregation replaces PROC SQL, var('report_month') replaces &report_month,
    SAS "calculated" column references replaced by inline expressions
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

-- SAS: PROC SQL CREATE TABLE REPORTS.LLP_COVERAGE → dbt: SELECT with GROUP BY
llp as (
    select
        {{ var('report_month') }}                   as report_month,
        a.account_type,
        count(*)                                    as n_loans,
        sum(a.current_balance)                      as gross_loans,
        sum(l.allowance_amt)                        as total_allowance,

        -- SAS: COVERAGE_PCT = allowance / gross_loans * 100
        case
            when sum(a.current_balance) > 0
                then sum(l.allowance_amt) / sum(a.current_balance) * 100
            else 0
        end                                         as coverage_pct,

        -- SAS: NPL_BALANCE = sum of balances where DAYS_PAST_DUE >= 90
        sum(
            case when l.days_past_due >= 90 then a.current_balance else 0 end
        )                                           as npl_balance,

        -- SAS: NPL_COVERAGE_PCT = allowance / NPL_BALANCE * 100
        case
            when sum(case when l.days_past_due >= 90 then a.current_balance else 0 end) > 0
                then sum(l.allowance_amt)
                     / sum(case when l.days_past_due >= 90 then a.current_balance else 0 end)
                     * 100
            else 0
        end                                         as npl_coverage_pct

    from accounts a
    -- SAS: INNER JOIN (not LEFT JOIN) for LLP
    inner join loan_details l
        on a.account_id = l.account_id
    -- SAS: WHERE a.SNAPSHOT_DATE = "&month_end"d AND lending types
    where a.last_activity_date <= last_day(to_date({{ var('report_month') }} || '01', 'yyyyMMdd'))
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
    group by 1, 2
)

select * from llp
