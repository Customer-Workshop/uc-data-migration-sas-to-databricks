/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL aggregating delinquency aging buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+)
    for lending accounts only. Joined STG_BANK.CUST_ACCOUNTS_DAILY
    with ORA_DW.LOAN_DETAILS for DAYS_PAST_DUE.

  dbt Equivalent:
    SQL CASE replaces SAS bucket assignment
    SQL GROUP BY replaces PROC SQL aggregation
    ref('int_account_metrics') replaces STG_BANK.CUST_ACCOUNTS_DAILY
    source('banking_raw', 'loan_details') replaces ORA_DW.LOAN_DETAILS
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

-- SAS: PROC SQL with delinquency bucket CASE
bucketed as (
    select
        {{ var('prev_ym') }} as report_month,
        a.account_type,
        a.region_code,
        case
            when l.days_past_due = 0                      then 'Current'
            when l.days_past_due between 1 and 29         then '1-29'
            when l.days_past_due between 30 and 59        then '30-59'
            when l.days_past_due between 60 and 89        then '60-89'
            when l.days_past_due between 90 and 119       then '90-119'
            when l.days_past_due between 120 and 179      then '120-179'
            when l.days_past_due >= 180                   then '180+'
            else 'Unknown'
        end as delinq_bucket,
        a.current_balance,
        l.past_due_amount
    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
),

aggregated as (
    select
        report_month,
        account_type,
        region_code,
        delinq_bucket,
        count(*) as n_accounts,
        sum(current_balance) as total_balance,
        sum(coalesce(past_due_amount, 0)) as total_past_due
    from bucketed
    group by 1, 2, 3, 4
)

select * from aggregated
