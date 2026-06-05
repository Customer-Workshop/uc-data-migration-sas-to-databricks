/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING from STG_BANK.CUST_ACCOUNTS_DAILY
    LEFT JOIN ORA_DW.LOAN_DETAILS. Delinquency bucketing by days past due,
    filtered to lending account types, grouped by REPORT_MONTH, ACCOUNT_TYPE,
    REGION_CODE, DELINQ_BUCKET.

  dbt Equivalent:
    SQL model reading from int_account_metrics joined to payment_history.
    CASE expression mirrors the SAS delinquency bucketing value-for-value.

  Column mapping:
    - SAS DAYS_PAST_DUE (ORA_DW.LOAN_DETAILS) -> payment_history.max_days_past_due_ever
    - SAS PAST_DUE_AMOUNT (STG_BANK.CUST_ACCOUNTS_DAILY) -> derived: balance
      where delinquent. The source table carried this as a first-class column;
      the dbt raw layer does not seed it, so we proxy it from balance x
      delinquency status. Flagged as a derivation.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

payment as (
    select * from {{ source('banking_raw', 'payment_history') }}
),

joined as (
    select
        a.account_id,
        a.account_type,
        a.region_code,
        a.current_balance,
        coalesce(p.max_days_past_due_ever, 0) as days_past_due,
        /* SAS: PAST_DUE_AMOUNT from STG_BANK.CUST_ACCOUNTS_DAILY.
           Not seeded in raw layer; proxied from balance when delinquent. */
        case
            when coalesce(p.max_days_past_due_ever, 0) > 0
            then a.current_balance
            else 0
        end as past_due_amount
    from accounts a
    left join payment p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

bucketed as (
    select
        {{ var('prev_ym') }} as report_month,
        account_type,
        region_code,
        case
            when days_past_due = 0              then 'Current'
            when days_past_due between 1 and 29     then '1-29'
            when days_past_due between 30 and 59    then '30-59'
            when days_past_due between 60 and 89    then '60-89'
            when days_past_due between 90 and 119   then '90-119'
            when days_past_due between 120 and 179  then '120-179'
            when days_past_due >= 180           then '180+'
            else 'Unknown'
        end as delinq_bucket,
        current_balance,
        past_due_amount
    from joined
)

select
    report_month,
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
