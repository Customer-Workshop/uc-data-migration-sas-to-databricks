/*
  mart_risk_scores.sql
  Migrated from: Programs/Banking/credit_risk_scoring.sas (Steps 1, 2, 4)
  Output contract: CURATED.RISK_SCORES  (WORK.SCORED appended with FORCE)

  SAS Original:
    Step 1  WORK.SCORE_INPUT — today's snapshot (SNAPSHOT_DATE = "&score_date"d,
            lending products only) LEFT JOIN
              ORA_DW.BUREAU_SCORES b on CUSTOMER_ID and
                b.SCORE_DATE = (select max(SCORE_DATE) from BUREAU_SCORES
                                where CUSTOMER_ID = b.CUSTOMER_ID
                                  and SCORE_DATE <= "&score_date"d)
              ORA_DW.PAYMENT_HISTORY p on ACCOUNT_ID
              ORA_DW.COLLATERAL c on ACCOUNT_ID
            LTV = CURRENT_BALANCE / COLLATERAL_VALUE when COLLATERAL_VALUE > 0.
    Step 2  DATA WORK.SCORED — scorecard CRM-2023-Q4-v2 (WOE bins, log-odds,
            PD, LGD, EAD, EXPECTED_LOSS, NEW_RISK_RATING); drops INTERCEPT,
            WOE_* and LOG_ODDS.

  dbt Equivalent:
    The correlated "latest score on or before score_date" sub-select becomes a
    window MAX over the date-filtered bureau table; ties on the max date fan
    out exactly as they would in SAS. All IF/ELSE ladders are CASE expressions
    in the same order, with the SAS `not missing()` guards made explicit.
    Column order and names follow WORK.SCORED.
*/

{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'score_date'],
        incremental_strategy='merge'
    )
}}

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_run_date() }}
      and account_type in {{ sas_lending_products() }}
),

-- latest bureau pull on or before the score date, per customer
bureau_latest as (
    select *
    from (
        select
            *,
            max(score_date) over (partition by customer_id) as max_score_date
        from {{ source('banking_raw', 'bureau_scores') }}
        where score_date <= {{ sas_run_date() }}
    )
    where score_date = max_score_date
),

-- SAS: WORK.SCORE_INPUT
score_input as (
    select
        a.account_id,
        a.customer_id,
        a.account_type,
        a.current_balance,
        a.credit_limit,
        a.acct_age_months,
        a.days_inactive,
        a.utilization_pct,
        a.customer_segment,
        a.region_code,
        b.fico_score,
        b.vantage_score,
        b.bureau_inqs_6mo,
        b.bureau_trades_open,
        b.bureau_derogs,
        b.bureau_util_pct,
        b.bureau_oldest_trade_mo,
        p.pmt_ontime_12mo,
        p.pmt_late_30_12mo,
        p.pmt_late_60_12mo,
        p.pmt_late_90_12mo,
        p.max_days_past_due_ever,
        p.months_since_last_dpd,
        p.avg_pmt_ratio_12mo,
        c.collateral_value,
        c.last_appraisal_date,
        case
            when c.collateral_value > 0 then a.current_balance / c.collateral_value
            else null
        end as ltv
    from accounts a
    left join bureau_latest b
        on a.customer_id = b.customer_id
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

-- SAS: DATA WORK.SCORED — Weight-of-Evidence bins
woe as (
    select
        *,
        -3.2145 as intercept,
        case
            when fico_score is null then 0.198  -- population average for missing
            when fico_score >= 760 then -1.204
            when fico_score >= 720 then -0.812
            when fico_score >= 680 then -0.356
            when fico_score >= 640 then 0.198
            when fico_score >= 600 then 0.654
            else 1.102
        end as woe_fico,
        case
            when utilization_pct is null then 0
            when utilization_pct <= 10 then -0.956
            when utilization_pct <= 30 then -0.521
            when utilization_pct <= 50 then -0.102
            when utilization_pct <= 70 then 0.334
            when utilization_pct <= 90 then 0.789
            else 1.245
        end as woe_util,
        case
            when pmt_late_90_12mo is null then 0
            when pmt_late_90_12mo = 0 then -0.678
            when pmt_late_90_12mo = 1 then 0.445
            else 1.567
        end as woe_dpd,
        case
            when acct_age_months is null then 0
            when acct_age_months >= 120 then -0.534
            when acct_age_months >= 60 then -0.289
            when acct_age_months >= 24 then 0.045
            else 0.456
        end as woe_age,
        case
            when account_type not in {{ sas_secured_products() }} then 0
            when ltv is null then 0
            when ltv <= 0.60 then -0.712
            when ltv <= 0.80 then -0.234
            when ltv <= 1.00 then 0.356
            else 0.889
        end as woe_ltv
    from score_input
),

log_odds as (
    select
        *,
        intercept
        + 0.412 * woe_fico
        + 0.198 * woe_util
        + 0.289 * woe_dpd
        + 0.067 * woe_age
        + 0.134 * woe_ltv as log_odds
    from woe
),

scored as (
    select
        *,
        1 / (1 + exp(-log_odds)) as pd,
        case
            when account_type in {{ sas_secured_products() }} and ltv is not null
                then greatest(0, least(1, (ltv - 0.5) * 0.8))
            when account_type in {{ sas_secured_products() }} then 0.40
            when account_type = 'CC' then 0.75
            else 0.50
        end as lgd,
        case
            when account_type in {{ sas_revolving_products() }}
                then current_balance + 0.50 * (credit_limit - current_balance)
            else current_balance
        end as ead
    from log_odds
),

rated as (
    select
        *,
        pd * lgd * ead as expected_loss,
        case
            when pd < 0.005 then 1
            when pd < 0.01 then 2
            when pd < 0.03 then 3
            when pd < 0.07 then 4
            when pd < 0.15 then 5
            when pd < 0.30 then 6
            else 7
        end as new_risk_rating
    from scored
)

select
    account_id,
    customer_id,
    account_type,
    current_balance,
    credit_limit,
    acct_age_months,
    days_inactive,
    utilization_pct,
    customer_segment,
    region_code,
    fico_score,
    vantage_score,
    bureau_inqs_6mo,
    bureau_trades_open,
    bureau_derogs,
    bureau_util_pct,
    bureau_oldest_trade_mo,
    pmt_ontime_12mo,
    pmt_late_30_12mo,
    pmt_late_60_12mo,
    pmt_late_90_12mo,
    max_days_past_due_ever,
    months_since_last_dpd,
    avg_pmt_ratio_12mo,
    collateral_value,
    last_appraisal_date,
    ltv,
    pd,
    lgd,
    ead,
    expected_loss,
    new_risk_rating,
    {{ sas_run_date() }} as score_date,
    '{{ var("model_id", "CRM-2023-Q4-v2") }}' as model_id,
    current_timestamp() as score_timestamp,
    {{ format_risk_rating('new_risk_rating') }} as new_risk_rating_desc,
    {{ format_account_type('account_type') }} as account_type_desc
from rated
