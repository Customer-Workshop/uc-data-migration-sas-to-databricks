/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL with CASE-based delinquency bucket assignment and
    GROUP BY aggregation for lending accounts only.

  dbt Equivalent:
    SQL CASE replaces SAS bucket logic.
    SAS "calculated" column → CTE so the bucket is referenceable.
    payment_history.max_days_past_due_ever maps to SAS DAYS_PAST_DUE.
*/

with lending_accounts as (
    select
        a.account_id,
        a.account_type,
        a.region_code,
        a.current_balance,
        coalesce(p.max_days_past_due_ever, 0) as days_past_due
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

bucketed as (
    select
        *,
        case
            when days_past_due = 0                 then 'Current'
            when days_past_due between 1 and 29    then '1-29'
            when days_past_due between 30 and 59   then '30-59'
            when days_past_due between 60 and 89   then '60-89'
            when days_past_due between 90 and 119  then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180              then '180+'
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
    sum(current_balance) as total_balance
from bucketed
group by 1, 2, 3, 4
