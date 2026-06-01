/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    PROC MEANS with CLASS POLICY_TYPE, aggregating YTD_EARNED_PREMIUM,
    TOTAL_INCURRED, TOTAL_PAID, TOTAL_RESERVE, IBNR_ESTIMATE.
    Followed by a DATA step adding AGG_LOSS_RATIO and AGG_COMBINED_RATIO.

  dbt Equivalent:
    SQL GROUP BY replaces PROC MEANS CLASS statement.
    Calculated ratios added in same query (no need for two-pass DATA step).

  Quirk reproduced from source (flagged, not fixed):
    AGG_COMBINED_RATIO uses the same hard-coded 30% expense load as the
    policy-level calculation (line 188 in SAS). Source-faithful.
*/

with policy_vals as (
    select * from {{ ref('int_policy_valuation') }}
),

aggregated as (
    /* SAS: PROC MEANS ... CLASS POLICY_TYPE */
    select
        pv.policy_type,
        count(*) as n_policies,
        sum(pv.ytd_earned_premium) as total_earned,
        sum(coalesce(pv.total_incurred, 0)) as total_incurred,
        sum(coalesce(pv.total_paid, 0)) as total_paid,
        sum(pv.total_reserve) as total_reserves,
        sum(pv.ibnr_estimate) as total_ibnr
    from policy_vals pv
    group by pv.policy_type
),

with_ratios as (
    /* SAS: DATA step adding AGG_LOSS_RATIO, AGG_COMBINED_RATIO */
    select
        policy_type,
        n_policies,
        total_earned,
        total_incurred,
        total_paid,
        total_reserves,
        total_ibnr,

        -- SAS: AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED
        case
            when total_earned > 0
                then total_incurred / total_earned
            else null
        end as agg_loss_ratio,

        -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30
        -- QUIRK: hard-coded 30% expense load (source-faithful)
        case
            when total_earned > 0
                then total_incurred / total_earned + 0.30
            else null
        end as agg_combined_ratio

    from aggregated
)

select * from with_ratios
