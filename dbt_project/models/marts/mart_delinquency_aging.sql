/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2, lines 72-109)

  SAS Original:
    PROC SQL joining STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS,
    bucketing DAYS_PAST_DUE into delinquency categories via CASE,
    filtering to lending account types, aggregating by report_month / account_type / region_code / delinq_bucket

  dbt Equivalent:
    SQL CASE replaces SAS bucket assignment, SQL GROUP BY replaces PROC SQL aggregation,
    var('report_month') replaces &report_month
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

-- SAS: PROC SQL CREATE TABLE REPORTS.DELINQUENCY_AGING → dbt: SELECT with GROUP BY
aging as (
    select
        {{ var('report_month') }}                   as report_month,
        a.account_type,
        a.region_code,

        -- SAS: CASE buckets for DAYS_PAST_DUE
        case
            when l.days_past_due = 0                then 'Current'
            when l.days_past_due between 1 and 29   then '1-29'
            when l.days_past_due between 30 and 59  then '30-59'
            when l.days_past_due between 60 and 89  then '60-89'
            when l.days_past_due between 90 and 119 then '90-119'
            when l.days_past_due between 120 and 179 then '120-179'
            when l.days_past_due >= 180              then '180+'
            else 'Unknown'
        end                                         as delinq_bucket,

        count(*)                                    as n_accounts,
        sum(a.current_balance)                      as total_balance,
        sum(l.past_due_amount)                      as total_past_due

    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
    -- SAS: WHERE a.SNAPSHOT_DATE = "&month_end"d AND a.ACCOUNT_TYPE IN (lending types)
    where a.last_activity_date <= last_day(to_date({{ var('report_month') }} || '01', 'yyyyMMdd'))
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
    group by
        report_month,
        a.account_type,
        a.region_code,
        delinq_bucket
)

select * from aging
order by
    account_type,
    region_code,
    -- SAS: ORDER BY with CASE for bucket sort order
    case
        when delinq_bucket = 'Current'  then 0
        when delinq_bucket = '1-29'     then 1
        when delinq_bucket = '30-59'    then 2
        when delinq_bucket = '60-89'    then 3
        when delinq_bucket = '90-119'   then 4
        when delinq_bucket = '120-179'  then 5
        when delinq_bucket = '180+'     then 6
        else 7
    end
