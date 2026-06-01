/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5: REPORTS.LOSS_RATIO_SUMMARY)

  SAS Original:
    PROC MEANS / PROC SQL aggregation of the policy valuation table by
    line of business (POLICY_TYPE).

  dbt Equivalent:
    PROC MEANS aggregation becomes a SQL GROUP BY over int_policy_valuation.
*/

with valuation as (
    select * from {{ ref('int_policy_valuation') }}
)

select
    policy_type,
    count(*) as n_policies,
    sum(ytd_earned_premium) as total_earned,
    sum(total_incurred) as total_incurred,
    -- aggregate loss ratio = total incurred / total earned
    case
        when sum(ytd_earned_premium) > 0
            then sum(total_incurred) / sum(ytd_earned_premium)
        else null
    end as agg_loss_ratio,
    -- aggregate combined ratio = loss ratio + 30% expense load
    case
        when sum(ytd_earned_premium) > 0
            then sum(total_incurred) / sum(ytd_earned_premium) + 0.30
        else null
    end as agg_combined_ratio
from valuation
group by policy_type
