/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL creating REPORTS.DELINQUENCY_AGING — delinquency aging buckets
    (Current / 1-29 / 30-59 / 60-89 / 90-119 / 120-179 / 180+) for credit
    products only. Reads the daily account snapshot STG_BANK.CUST_ACCOUNTS_DAILY
    LEFT JOIN ORA_DW.LOAN_DETAILS, filtered to SNAPSHOT_DATE = month_end and
    ACCOUNT_TYPE in ('MTG','AUTO','PERS','CC','LOC','HELC'). Aggregates by
    account type, region, and bucket.

  dbt Equivalent:
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY. The CASE
    reproduces the SAS bucket mapping value-for-value; per-value fidelity is
    gated by tests/reconcile_delinquency_bucket_parity.sql.

  Data-source mapping notes (flagged, not logic changes):
    - SAS reads DAYS_PAST_DUE from the daily snapshot. The Databricks raw schema
      has no current days_past_due column; the closest available signal is
      raw.payment_history.max_days_past_due_ever, used here as the bucketing
      input. This is a coarser "worst ever" measure rather than the current
      month-end value — a documented data gap, not a remediation.
    - SAS sums PAST_DUE_AMOUNT. There is no past-due-amount column in the
      Databricks raw schema, so total_past_due is reported as 0 (a flagged data
      gap). The reconciliation control ties this out to the (absent) source.

  Source-faithful quirks reproduced (flagged, NOT corrected):
    - The explicit 'Unknown' else branch catches NULL / negative days_past_due.
      Preserved for fidelity even where the data does not trigger it.
    - The SAS SNAPSHOT_DATE = month_end filter has no equivalent here: the dbt
      snapshot is point-in-time (no time-series snapshots are materialized).
*/

with accounts as (
    select
        account_id,
        account_type,
        region_code,
        current_balance
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

payment_history as (
    select
        account_id,
        max_days_past_due_ever as days_past_due
    from {{ source('banking_raw', 'payment_history') }}
),

-- Delinquency bucket per account — mirrors the SAS CASE in Step 2 exactly.
bucketed as (
    select
        '{{ var("prev_ym") }}' as report_month,
        a.account_type,
        a.region_code,
        a.current_balance,
        -- PAST_DUE_AMOUNT has no source column in the Databricks raw schema.
        cast(0 as double) as past_due_amount,
        case
            when p.days_past_due = 0 then 'Current'
            when p.days_past_due between 1 and 29 then '1-29'
            when p.days_past_due between 30 and 59 then '30-59'
            when p.days_past_due between 60 and 89 then '60-89'
            when p.days_past_due between 90 and 119 then '90-119'
            when p.days_past_due between 120 and 179 then '120-179'
            when p.days_past_due >= 180 then '180+'
            -- SAS: else 'Unknown' — catches NULL days_past_due (source-faithful).
            else 'Unknown'
        end as delinq_bucket
    from accounts a
    left join payment_history p
        on a.account_id = p.account_id
),

-- Aggregate to the reporting grain (SAS: group by 1,2,3,4).
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
)

select
    report_month,
    account_type,
    region_code,
    delinq_bucket,
    n_accounts,
    total_balance,
    total_past_due
from grouped
