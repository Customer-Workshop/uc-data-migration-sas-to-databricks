/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS with CLASS POLICY_TYPE, aggregating premium/incurred/
    paid/reserve/IBNR, followed by DATA step computing aggregate
    loss and combined ratios per line of business.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS.
    CASE expressions compute ratios inline.
*/

with policy_vals as (
    select * from {{ ref('int_policy_valuation') }}
)

select
    policy_type,
    count(*) as n_policies,
    sum(ytd_earned_premium) as total_earned,
    sum(coalesce(total_incurred, 0)) as total_incurred,
    sum(coalesce(total_paid, 0)) as total_paid,
    sum(total_reserve) as total_reserves,
    sum(ibnr_estimate) as total_ibnr,

    -- SAS: AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED
    case
        when sum(ytd_earned_premium) > 0
            then sum(coalesce(total_incurred, 0))
                 / sum(ytd_earned_premium)
        else null
    end as agg_loss_ratio,

    -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30
    case
        when sum(ytd_earned_premium) > 0
            then sum(coalesce(total_incurred, 0))
                 / sum(ytd_earned_premium) + 0.30
        else null
    end as agg_combined_ratio

from policy_vals
group by policy_type
