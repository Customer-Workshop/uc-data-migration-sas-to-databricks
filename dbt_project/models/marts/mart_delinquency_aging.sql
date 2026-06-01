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
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY and carries
    days_past_due / past_due_amount forward from the raw account snapshot (the
    same place the SAS query reads the unqualified DAYS_PAST_DUE /
    PAST_DUE_AMOUNT columns). The SAS LEFT JOIN to ORA_DW.LOAN_DETAILS is not
    reproduced because Step 2 reads no loan_details columns — it is a left join
    on a unique key, so dropping it changes neither the row population nor the
    measures. The CASE reproduces the SAS bucket mapping value-for-value; per-
    value fidelity is gated by tests/reconcile_delinquency_bucket_parity.sql.

  Source-faithful quirks reproduced (flagged, NOT corrected):
    - The explicit 'Unknown' else branch catches NULL / negative days_past_due.
      In the current raw data every in-scope account has days_past_due >= 0, so
      this branch does not fire, but it is preserved for fidelity.
    - The SAS SNAPSHOT_DATE = month_end filter has no equivalent here: the dbt
      snapshot is point-in-time (no time-series snapshots are materialized).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- Delinquency bucket per account — mirrors the SAS CASE in Step 2 exactly.
bucketed as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        region_code,
        current_balance,
        coalesce(past_due_amount, 0) as past_due_amount,
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            -- SAS: else 'Unknown' — catches NULL days_past_due (source-faithful).
            else 'Unknown'
        end as delinq_bucket
    from accounts
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
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
