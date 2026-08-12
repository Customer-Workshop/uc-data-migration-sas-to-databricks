/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL aggregation over STG_BANK.CUST_ACCOUNTS_DAILY left joined to
    ORA_DW.LOAN_DETAILS, bucketing DAYS_PAST_DUE into aging bands and
    grouping by REPORT_MONTH / ACCOUNT_TYPE / REGION_CODE / DELINQ_BUCKET.
    Scope: lending products only ('MTG','AUTO','PERS','CC','LOC','HELC').

  dbt Equivalent:
    int_account_metrics is the CUST_ACCOUNTS_DAILY equivalent.
    The bucket CASE mirrors the SAS bands value-for-value, including the
    'Unknown' catch-all: a missing DAYS_PAST_DUE (no loan record) falls to
    'Unknown' in SAS and to the else branch here — same behaviour.
    The SAS ORDER BY bucket-rank is presentation-only and is exposed here
    as bucket_sort_order for downstream consumers.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loans as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

bucketed as (
    select
        a.account_type,
        a.region_code,
        a.current_balance,
        l.past_due_amount,
        case
            when l.days_past_due = 0 then 'Current'
            when l.days_past_due between 1 and 29 then '1-29'
            when l.days_past_due between 30 and 59 then '30-59'
            when l.days_past_due between 60 and 89 then '60-89'
            when l.days_past_due between 90 and 119 then '90-119'
            when l.days_past_due between 120 and 179 then '120-179'
            when l.days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from accounts a
    left join loans l
        on a.account_id = l.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
)

select
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    region_code,
    delinq_bucket,
    case delinq_bucket
        when 'Current' then 0
        when '1-29' then 1
        when '30-59' then 2
        when '60-89' then 3
        when '90-119' then 4
        when '120-179' then 5
        when '180+' then 6
        else 7
    end as bucket_sort_order,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due
from bucketed
group by account_type, region_code, delinq_bucket
