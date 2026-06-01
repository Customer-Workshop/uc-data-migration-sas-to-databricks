/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS ... CLASS POLICY_TYPE; aggregating YTD_EARNED_PREMIUM,
    TOTAL_INCURRED, TOTAL_PAID, TOTAL_RESERVE, IBNR_ESTIMATE with
    N= and SUM= statistics.
    Followed by a DATA step adding AGG_LOSS_RATIO and AGG_COMBINED_RATIO.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS CLASS statement.
    Derived ratios computed inline (no second pass needed).

  Quirks reproduced from source (flagged, not fixed):
    [Q1] AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30 (hard-coded expense load).
    [Q5] AGG_LOSS_RATIO and AGG_COMBINED_RATIO are NULL when TOTAL_EARNED = 0,
         matching the SAS IF TOTAL_EARNED > 0 guard.
*/

with policy_valuation as (
    select * from {{ ref('int_policy_valuation') }}
)

select
    policy_type,

    -- SAS: N=N_POLICIES (count of observations per CLASS)
    count(*) as n_policies,

    -- SAS: SUM(YTD_EARNED_PREMIUM)=TOTAL_EARNED
    sum(ytd_earned_premium) as total_earned,

    -- SAS: SUM(TOTAL_INCURRED)=TOTAL_INCURRED
    sum(total_incurred) as total_incurred,

    -- SAS: SUM(TOTAL_PAID)=TOTAL_PAID
    sum(total_paid) as total_paid,

    -- SAS: SUM(TOTAL_RESERVE)=TOTAL_RESERVES
    sum(total_reserve) as total_reserves,

    -- SAS: SUM(IBNR_ESTIMATE)=TOTAL_IBNR
    sum(ibnr_estimate) as total_ibnr,

    -- SAS: AGG_LOSS_RATIO (guarded by TOTAL_EARNED > 0) [Q5]
    case
        when sum(ytd_earned_premium) > 0
            then sum(total_incurred) / sum(ytd_earned_premium)
        else null
    end as agg_loss_ratio,

    -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30 [Q1] [Q5]
    case
        when sum(ytd_earned_premium) > 0
            then (sum(total_incurred) / sum(ytd_earned_premium)) + 0.30
        else null
    end as agg_combined_ratio

from policy_valuation
group by policy_type
