/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL computing delinquency aging buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+)
    aggregated by account_type and region_code. Joins
    STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS for
    DAYS_PAST_DUE and PAST_DUE_AMOUNT.

  dbt Equivalent:
    SQL CASE replaces SAS bucket assignment.
    SQL GROUP BY replaces PROC SQL aggregation.
    SAS PROC FORMAT DELQBKT replaced by inline CASE.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loans as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

bucketed as (
    select
        {{ var('prev_ym') }} as report_month,
        a.account_type,
        a.region_code,
        case
            when l.days_past_due = 0                         then 'Current'
            when l.days_past_due between 1 and 29            then '1-29'
            when l.days_past_due between 30 and 59           then '30-59'
            when l.days_past_due between 60 and 89           then '60-89'
            when l.days_past_due between 90 and 119          then '90-119'
            when l.days_past_due between 120 and 179         then '120-179'
            when l.days_past_due >= 180                      then '180+'
            else 'Unknown'
        end as delinq_bucket,
        a.current_balance,
        l.past_due_amount
    from accounts a
    left join loans l
        on a.account_id = l.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
)

select
    report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*)                  as n_accounts,
    sum(current_balance)      as total_balance,
    sum(past_due_amount)      as total_past_due
from bucketed
group by
    report_month,
    account_type,
    region_code,
    delinq_bucket
order by
    account_type,
    region_code,
    case delinq_bucket
        when 'Current'  then 0
        when '1-29'     then 1
        when '30-59'    then 2
        when '60-89'    then 3
        when '90-119'   then 4
        when '120-179'  then 5
        when '180+'     then 6
        else 7
    end
