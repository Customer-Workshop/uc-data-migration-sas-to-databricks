/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING — delinquency aging buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+).
    Sources: STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN ORA_DW.LOAN_DETAILS,
    filtered to month-end snapshot and credit product types only.

  dbt Equivalent:
    SQL CASE expression reproduces the SAS delinquency bucket mapping
    value-for-value.  int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY.
    DAYS_PAST_DUE maps to payment_history.max_days_past_due_ever (the SAS
    ORA_DW.LOAN_DETAILS.DAYS_PAST_DUE is not a direct column in the
    Databricks raw schema).

  Source-faithful notes:
    - The SAS CASE includes an explicit 'Unknown' else branch for NULL or
      negative DAYS_PAST_DUE.  Reproduced here faithfully.
    - The account type filter ('MTG','AUTO','PERS','CC','LOC','HELC')
      excludes deposit-type accounts — source-faithful.
    - PAST_DUE_AMOUNT is not available in the Databricks raw schema.
      Defaulted to 0 when missing — flagged as a data gap.
*/

with accounts as (
    select
        a.account_id,
        a.account_type,
        a.region_code,
        a.current_balance
    from {{ ref('int_account_metrics') }} a
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

payment_info as (
    select
        account_id,
        max_days_past_due_ever as days_past_due
    from {{ source('banking_raw', 'payment_history') }}
),

aging_detail as (
    select
        '{{ var("prev_ym") }}' as report_month,
        a.account_type,
        a.region_code,
        /* ── Delinquency bucket CASE — source-faithful reproduction ──
           Every branch mirrors monthly_regulatory_reporting.sas Step 2
           exactly.  See reconciliation test reconcile_delinquency_bucket_parity
           for per-value verification against the source mapping. */
        case
            when p.days_past_due = 0                        then 'Current'
            when p.days_past_due between 1 and 29           then '1-29'
            when p.days_past_due between 30 and 59          then '30-59'
            when p.days_past_due between 60 and 89          then '60-89'
            when p.days_past_due between 90 and 119         then '90-119'
            when p.days_past_due between 120 and 179        then '120-179'
            when p.days_past_due >= 180                     then '180+'
            -- Source-faithful: catches NULL / negative days_past_due
            else 'Unknown'
        end as delinq_bucket,
        a.current_balance,
        -- PAST_DUE_AMOUNT not available in Databricks raw; defaulted to 0
        0 as past_due_amount
    from accounts a
    left join payment_info p
        on a.account_id = p.account_id
)

select
    report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(past_due_amount) as total_past_due
from aging_detail
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
