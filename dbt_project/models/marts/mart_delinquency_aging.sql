/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING from STG_BANK.CUST_ACCOUNTS_DAILY
    left joined to ORA_DW.LOAN_DETAILS, bucketing DAYS_PAST_DUE and grouping by
    REPORT_MONTH / ACCOUNT_TYPE / REGION_CODE / DELINQ_BUCKET.

  dbt Equivalent:
    The CASE bucket ladder and the lending-product WHERE are reproduced
    branch-for-branch; PROC SQL's GROUP BY becomes a SQL group by.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
      and account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

joined as (
    select
        a.account_type,
        a.region_code,
        a.current_balance,
        l.days_past_due,
        l.past_due_amount
    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
),

bucketed as (
    select
        *,
        -- Source-faithful: an account with no LOAN_DETAILS match has
        -- DAYS_PAST_DUE missing, which matches none of the SAS comparisons
        -- (a missing value is not 0 and is not >= 180) and lands in 'Unknown'.
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from joined
)

select
    {{ sas_report_month() }} as report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due
from bucketed
group by account_type, region_code, delinq_bucket
order by
    account_type,
    region_code,
    case
        when delinq_bucket = 'Current' then 0
        when delinq_bucket = '1-29' then 1
        when delinq_bucket = '30-59' then 2
        when delinq_bucket = '60-89' then 3
        when delinq_bucket = '90-119' then 4
        when delinq_bucket = '120-179' then 5
        when delinq_bucket = '180+' then 6
        else 7
    end
