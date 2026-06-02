/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  SAS Original:
    PROC SQL aggregating STG_BANK.CUST_ACCOUNTS_DAILY (left join ORA_DW.LOAN_DETAILS)
    for the month-end snapshot, restricted to lending products
    ('MTG','AUTO','PERS','CC','LOC','HELC'), assigning a delinquency aging bucket
    from DAYS_PAST_DUE, then grouping by ACCOUNT_TYPE, REGION_CODE and bucket to
    produce N_ACCOUNTS, TOTAL_BALANCE and TOTAL_PAST_DUE.

  dbt Equivalent:
    PROC SQL GROUP BY -> dbt SQL model with group by.
    SAS bucket CASE is reproduced value-for-value, including the catch-all
    else -> 'Unknown'.

  Source mapping notes (migrated raw schema vs. SAS Oracle schema):
    - STG_BANK.CUST_ACCOUNTS_DAILY -> int_account_metrics (staging scope already applied).
    - SAS read DAYS_PAST_DUE from ORA_DW.LOAN_DETAILS; the migrated raw estate has
      no such column, so days-past-due is taken from payment_history.max_days_past_due_ever,
      the same delinquency measure mart_risk_scores.sql uses.
    - SAS summed PAST_DUE_AMOUNT from ORA_DW.LOAN_DETAILS. The migrated raw estate
      has NO past-due-amount column, so total_past_due is reported as the at-risk
      exposure of delinquent accounts (current_balance where days_past_due > 0).
      This is a flagged data-availability gap, NOT a SAS logic change: there is no
      source column to reconcile against, so no parity control asserts on it.
*/

with accounts as (
    select
        a.account_type,
        a.region_code,
        a.current_balance,

        -- SAS: DAYS_PAST_DUE from ORA_DW.LOAN_DETAILS; mapped to
        --      payment_history.max_days_past_due_ever in the migrated raw schema.
        p.max_days_past_due_ever as days_past_due

    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    -- SAS: where a.ACCOUNT_TYPE in ('MTG','AUTO','PERS','CC','LOC','HELC')
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

-- SAS: Step 2 delinquency aging bucket CASE, reproduced value-for-value
--      (30/60/90/120/180+ boundaries) including the catch-all else -> 'Unknown'.
bucketed as (
    select
        *,
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
    from accounts
)

select
    -- SAS: "&report_month" (YYYYMM); &PREV_YM -> dbt var prev_ym
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,

    -- SAS: sum(PAST_DUE_AMOUNT) — no source column in the migrated raw schema;
    --      reported as at-risk exposure of delinquent accounts (flagged gap).
    sum(case when days_past_due > 0 then current_balance else 0 end) as total_past_due
from bucketed
group by account_type, region_code, delinq_bucket
order by
    account_type,
    region_code,
    -- SAS: explicit bucket ordering (Current=0 ... 180+=6, else=7)
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
