/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS data=STG_INS.POLICY_VALUATION nway;
      class POLICY_TYPE;
      n=N_POLICIES  sum(YTD_EARNED_PREMIUM)=TOTAL_EARNED
      sum(TOTAL_INCURRED)=TOTAL_INCURRED  sum(TOTAL_PAID)=TOTAL_PAID
      sum(TOTAL_RESERVE)=TOTAL_RESERVES  sum(IBNR_ESTIMATE)=TOTAL_IBNR
    Then DATA step adds AGG_LOSS_RATIO and AGG_COMBINED_RATIO.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS nway + class.
    Inline CASE replaces the subsequent DATA step.
*/

with policy_val as (
    select * from {{ ref('int_policy_valuation') }}
),

summary as (
    select
        policy_type,
        count(*) as n_policies,
        sum(ytd_earned_premium) as total_earned,
        sum(total_incurred) as total_incurred,
        sum(total_paid) as total_paid,
        sum(total_reserve) as total_reserves,
        sum(ibnr_estimate) as total_ibnr
    from policy_val
    group by policy_type
)

select
    policy_type,
    n_policies,
    total_earned,
    total_incurred,
    total_paid,
    total_reserves,
    total_ibnr,

    -- SAS: if TOTAL_EARNED > 0 then AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED
    case
        when total_earned > 0
            then total_incurred / total_earned
        else null
    end as agg_loss_ratio,

    -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30
    case
        when total_earned > 0
            then total_incurred / total_earned + 0.30
        else null
    end as agg_combined_ratio

from summary
