/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING — delinquency aging buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+) for
    lending account types. Joins STG_BANK.CUST_ACCOUNTS_DAILY to
    ORA_DW.LOAN_DETAILS. Aggregates by account type, region, and bucket.

  dbt Equivalent:
    Reads int_account_metrics (which carries days_past_due and
    past_due_amount from the raw accounts table). CASE expression
    reproduces the SAS bucket mapping value-for-value. The SAS LEFT JOIN
    to ORA_DW.LOAN_DETAILS is dropped because days_past_due and
    past_due_amount originate from the accounts table, not loan_details.

  Quirks reproduced from source (flagged, not fixed):
    - The 'Unknown' bucket catches NULL or negative days_past_due. In the
      seed data, non-lending accounts have dpd=0 and are excluded by the
      WHERE filter, so 'Unknown' should not appear. The branch exists in
      the SAS source and is preserved for fidelity.
    - The SAS ORDER BY uses a secondary CASE to sort buckets numerically.
      This is cosmetic in a table materialization but reproduced for
      documentation.
    - The SAS filtered by SNAPSHOT_DATE = month_end; the dbt model uses
      the current point-in-time view (no time-series snapshots available).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- Apply the delinquency bucket CASE (mirrors SAS exactly)
bucketed as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        region_code,
        case
            when days_past_due = 0                  then 'Current'
            when days_past_due between 1 and 29     then '1-29'
            when days_past_due between 30 and 59    then '30-59'
            when days_past_due between 60 and 89    then '60-89'
            when days_past_due between 90 and 119   then '90-119'
            when days_past_due between 120 and 179  then '120-179'
            when days_past_due >= 180               then '180+'
            -- SAS: else 'Unknown' — catches NULL days_past_due
            else 'Unknown'
        end as delinq_bucket,
        current_balance,
        coalesce(past_due_amount, 0) as past_due_amount
    from accounts
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

-- Aggregate (mirrors SAS GROUP BY 1,2,3,4)
grouped as (
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
        -- SAS ordering: bucket sort by numeric rank
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
)

select * from grouped
