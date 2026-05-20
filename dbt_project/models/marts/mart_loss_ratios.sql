/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS aggregating by POLICY_TYPE from STG_INS.POLICY_VALUATION,
    followed by a DATA step computing AGG_LOSS_RATIO and
    AGG_COMBINED_RATIO.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS aggregation.
    Computed loss ratio / combined ratio inline via CASE expression.
*/

with valuations as (
    select * from {{ ref('int_policy_valuation') }}
)

select
    policy_type,
    count(*)                       as n_policies,
    sum(ytd_earned_premium)        as total_earned,
    sum(total_incurred)            as total_incurred,
    sum(total_paid)                as total_paid,
    sum(total_reserve)             as total_reserves,
    sum(ibnr_estimate)             as total_ibnr,

    -- SAS: AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED
    case
        when sum(ytd_earned_premium) > 0
        then sum(coalesce(total_incurred, 0)) / sum(ytd_earned_premium)
        else null
    end as agg_loss_ratio,

    -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30
    case
        when sum(ytd_earned_premium) > 0
        then (sum(coalesce(total_incurred, 0)) / sum(ytd_earned_premium)) + 0.30
        else null
    end as agg_combined_ratio

from valuations
group by policy_type
order by policy_type
