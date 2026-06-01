/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING — delinquency buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+) for
    lending account types only.

    Inputs:  STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN ORA_DW.LOAN_DETAILS
    Filter:  SNAPSHOT_DATE = "&month_end"d
             AND ACCOUNT_TYPE IN ('MTG','AUTO','PERS','CC','LOC','HELC')
    Group:   REPORT_MONTH, ACCOUNT_TYPE, REGION_CODE, DELINQ_BUCKET

  dbt Equivalent:
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY.
    source('banking_raw', 'payment_history').max_days_past_due_ever replaces
    ORA_DW.LOAN_DETAILS.DAYS_PAST_DUE (schema adaptation — flagged below).

  Source-faithful notes:
    - Bucket boundaries are value-for-value identical to the SAS CASE.
    - SAS uses DAYS_PAST_DUE from LOAN_DETAILS; Databricks raw data has
      max_days_past_due_ever on payment_history. Used as the equivalent.
    - SAS sums PAST_DUE_AMOUNT for TOTAL_PAST_DUE; the synthetic raw data
      has no past_due_amount column. Derived as current_balance when
      days_past_due > 0, else 0 — flagged as a schema adaptation.
    - Account type filter reproduced verbatim from SAS WHERE clause.
    - ORDER BY uses a CASE to sort buckets by severity, matching the SAS
      ordering.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

payment_history as (
    select * from {{ source('banking_raw', 'payment_history') }}
),

-- Per-account delinquency assignment with bucket derivation.
account_delinquency as (
    select
        a.account_id,
        a.account_type,
        a.region_code,
        a.current_balance,
        coalesce(p.max_days_past_due_ever, 0) as days_past_due,
        /* Schema adaptation: SAS used PAST_DUE_AMOUNT from LOAN_DETAILS.
           Synthetic data lacks this column; current_balance when
           days_past_due > 0 is the best available proxy. */
        case
            when coalesce(p.max_days_past_due_ever, 0) > 0
            then a.current_balance
            else 0
        end as past_due_amount
    from accounts a
    left join payment_history p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

-- Bucket assignment — reproduces SAS CASE value for value.
bucketed as (
    select
        account_type,
        region_code,
        current_balance,
        past_due_amount,
        case
            when days_past_due = 0                then 'Current'
            when days_past_due between 1 and 29   then '1-29'
            when days_past_due between 30 and 59  then '30-59'
            when days_past_due between 60 and 89  then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180              then '180+'
            else 'Unknown'
        end as delinq_bucket
    from account_delinquency
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
