/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2: REPORTS.DELINQUENCY_AGING)

  SAS Original:
    PROC SQL bucketing DAYS_PAST_DUE into Current / 1-29 / 30-59 / 60-89 /
    90-119 / 120-179 / 180+ for lending account types, with a custom ORDER BY
    on the bucket.

  dbt Equivalent:
    The SAS bucket CASE and PROC SQL GROUP BY translate directly. days_past_due
    is sourced from payment_history (SAS used a daily snapshot column).
*/

with accounts as (
    select
        account_id,
        account_type,
        region_code,
        current_balance
    from {{ ref('int_account_metrics') }}
    -- SAS: lending products only
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

payments as (
    select
        account_id,
        max_days_past_due_ever as days_past_due
    from {{ source('banking_raw', 'payment_history') }}
),

bucketed as (
    select
        date_format(current_date(), 'yyyyMM') as report_month,
        a.account_type,
        a.region_code,
        a.current_balance,
        case
            when coalesce(p.days_past_due, 0) = 0 then 'Current'
            when p.days_past_due between 1 and 29 then '1-29'
            when p.days_past_due between 30 and 59 then '30-59'
            when p.days_past_due between 60 and 89 then '60-89'
            when p.days_past_due between 90 and 119 then '90-119'
            when p.days_past_due between 120 and 179 then '120-179'
            when p.days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from accounts a
    left join payments p on a.account_id = p.account_id
)

select
    report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance
from bucketed
group by report_month, account_type, region_code, delinq_bucket
