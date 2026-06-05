/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL computing delinquency aging buckets (Current/1-29/30-59/60-89/
    90-119/120-179/180+/Unknown) for loan account types only.
    Joins STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS on ACCOUNT_ID.

  dbt Equivalent:
    SQL CASE replaces SAS bucket assignment. The SAS DAYS_PAST_DUE column
    maps to payment_history.max_days_past_due_ever in the raw schema.
    Scope limited to MTG/AUTO/PERS/CC/LOC/HELC (source-faithful filter).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

payment_history as (
    select * from {{ source('banking_raw', 'payment_history') }}
),

bucketed as (
    select
        a.account_type,
        a.region_code,
        a.current_balance,
        case
            when p.max_days_past_due_ever = 0 then 'Current'
            when p.max_days_past_due_ever between 1 and 29 then '1-29'
            when p.max_days_past_due_ever between 30 and 59 then '30-59'
            when p.max_days_past_due_ever between 60 and 89 then '60-89'
            when p.max_days_past_due_ever between 90 and 119 then '90-119'
            when p.max_days_past_due_ever between 120 and 179 then '120-179'
            when p.max_days_past_due_ever >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from accounts a
    left join payment_history p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

aggregated as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        region_code,
        delinq_bucket,
        count(*) as n_accounts,
        sum(current_balance) as total_balance,
        cast(0 as double) as total_past_due
    from bucketed
    group by 1, 2, 3, 4
)

select * from aggregated
