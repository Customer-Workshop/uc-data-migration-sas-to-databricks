/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS with CLASS POLICY_TYPE aggregating YTD_EARNED_PREMIUM,
    TOTAL_INCURRED, TOTAL_PAID, TOTAL_RESERVE, IBNR_ESTIMATE.
    Followed by DATA step computing aggregate loss/combined ratios.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS CLASS aggregation
    Computed columns inline replace the two-pass DATA step approach
*/

with policy_vals as (
    select * from {{ ref('int_policy_valuation') }}
),

-- SAS: PROC MEANS + DATA step combined into single aggregation
summary as (
    select
        policy_type,
        count(*) as n_policies,
        sum(ytd_earned_premium) as total_earned,
        sum(coalesce(total_incurred, 0)) as total_incurred,
        sum(coalesce(total_paid, 0)) as total_paid,
        sum(coalesce(total_reserve, 0)) as total_reserves,
        sum(coalesce(ibnr_estimate, 0)) as total_ibnr,

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

    from policy_vals
    group by policy_type
)

select * from summary
