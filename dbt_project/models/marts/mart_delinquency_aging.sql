/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING from STG_BANK.CUST_ACCOUNTS_DAILY
    LEFT JOIN ORA_DW.LOAN_DETAILS at the month-end snapshot, restricted to the
    six lending account types, bucketing DAYS_PAST_DUE and aggregating balance
    and past-due amount per account type / region / bucket.

  dbt Equivalent:
    SQL CASE replaces the SAS bucket assignment, GROUP BY replaces the PROC SQL
    aggregation, and the SAS ORDER BY bucket-rank expression becomes an
    explicit sort key.

  Source parity notes (see reconcile_delinquency_* tests):
    - Bucket boundaries mirror the SAS BETWEEN ranges exactly.
    - The join is a LEFT join, as in the source: a lending account with no
      LOAN_DETAILS row has a missing DAYS_PAST_DUE, which fails every SAS
      comparison and lands in the 'Unknown' bucket. NULL behaves the same way
      here, so those accounts stay visible rather than being dropped.
      Source-faithful, not an endorsement.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    -- SAS: where a.SNAPSHOT_DATE = "&month_end"d
    where snapshot_date = current_date()
      and account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
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

        -- SAS: CASE ... end as DELINQ_BUCKET
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
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due
from bucketed
group by report_month, account_type, region_code, delinq_bucket
-- SAS: ORDER BY ACCOUNT_TYPE, REGION_CODE, <bucket rank>
order by
    account_type,
    region_code,
    case delinq_bucket
        when 'Current' then 0
        when '1-29' then 1
        when '30-59' then 2
        when '60-89' then 3
        when '90-119' then 4
        when '120-179' then 5
        when '180+' then 6
        else 7
    end
