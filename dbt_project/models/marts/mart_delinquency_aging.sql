/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)
  Output contract: REPORTS.DELINQUENCY_AGING (replaced each run)

  SAS Original:
    Month-end snapshot, lending products only (MTG/AUTO/PERS/CC/LOC/HELC),
    LEFT JOIN ORA_DW.LOAN_DETAILS; DELINQ_BUCKET from DAYS_PAST_DUE:
      0 Current | 1-29 | 30-59 | 60-89 | 90-119 | 120-179 | >=180 '180+' | else Unknown
    (missing DAYS_PAST_DUE — no loan row — falls to Unknown in both engines).
    N_ACCOUNTS = count(*), TOTAL_BALANCE = sum(CURRENT_BALANCE),
    TOTAL_PAST_DUE = sum(PAST_DUE_AMOUNT)
    group by REPORT_MONTH, ACCOUNT_TYPE, REGION_CODE, DELINQ_BUCKET
    order by ACCOUNT_TYPE, REGION_CODE, severity (Current=0 ... 180+=6, Unknown=7)

  dbt Equivalent:
    Tables have no intrinsic order, so the severity rank used by the SAS
    ORDER BY is materialised as BUCKET_SORT_ORDER (an added column) and the
    Excel export sorts on it.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
      and account_type in {{ sas_lending_products() }}
),

loans as (
    select * from {{ ref('stg_loan_details') }}
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
)

select
    '{{ sas_report_month() }}' as report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due,
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
    {{ format_account_type('account_type') }} as account_type_desc,
    {{ format_region('region_code') }} as region_desc
from bucketed
group by account_type, region_code, delinq_bucket
