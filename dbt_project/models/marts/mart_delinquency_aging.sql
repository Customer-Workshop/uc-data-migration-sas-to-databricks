/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING
    Delinquency aging buckets for lending account types.
    Joined STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS for DAYS_PAST_DUE.

  dbt Equivalent:
    SQL CASE replaces SAS bucket assignment.
    DAYS_PAST_DUE sourced from payment_history.max_days_past_due_ever
    (SAS carried DAYS_PAST_DUE on LOAN_DETAILS directly).
    PAST_DUE_AMOUNT not available in seed data — column included as NULL
    and flagged below.
    Only lending account types in scope: MTG, AUTO, PERS, CC, LOC, HELC.

  Buckets (source-faithful):
    DAYS_PAST_DUE = 0           → Current
    1–29                        → 1-29
    30–59                       → 30-59
    60–89                       → 60-89
    90–119                      → 90-119
    120–179                     → 120-179
    >= 180                      → 180+
    else (NULL)                 → Unknown
*/

with accounts as (
    select * from {{ ref('stg_cust_accounts') }}
),

payment_history as (
    select * from {{ source('banking_raw', 'payment_history') }}
),

lending_accounts as (
    select
        a.account_id,
        a.account_type,
        a.region_code,
        a.current_balance,
        -- SAS: DAYS_PAST_DUE from LOAN_DETAILS; here from payment_history
        ph.max_days_past_due_ever as days_past_due,
        -- SAS: PAST_DUE_AMOUNT from LOAN_DETAILS — not available in seed data.
        -- Flagged: included as NULL for schema completeness.
        cast(null as double) as past_due_amount
    from accounts a
    left join payment_history ph
        on a.account_id = ph.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

bucketed as (
    select
        *,
        case
            when days_past_due = 0                        then 'Current'
            when days_past_due between 1 and 29           then '1-29'
            when days_past_due between 30 and 59          then '30-59'
            when days_past_due between 60 and 89          then '60-89'
            when days_past_due between 90 and 119         then '90-119'
            when days_past_due between 120 and 179        then '120-179'
            when days_past_due >= 180                     then '180+'
            else 'Unknown'
        end as delinq_bucket
    from lending_accounts
)

select
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due
from bucketed
group by
    report_month,
    account_type,
    region_code,
    delinq_bucket
order by
    account_type,
    region_code,
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
