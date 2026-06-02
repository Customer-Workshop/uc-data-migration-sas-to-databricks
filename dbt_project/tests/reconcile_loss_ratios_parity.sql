/*
  Reconciliation control: loss-ratio summary formula parity.

  Asserts the aggregate ratios in mart_loss_ratios match the SAS Step 5
  DATA-step formulas value-for-value, and that the summed components tie back
  to a fresh aggregation of int_policy_valuation (so the GROUP BY did not lose
  or double-count policies within a line of business):

    TOTAL_EARNED       = sum(YTD_EARNED_PREMIUM)                [SAS 175]
    TOTAL_INCURRED     = sum(TOTAL_INCURRED)                    [SAS 176]
    AGG_LOSS_RATIO     = TOTAL_INCURRED / TOTAL_EARNED          [SAS 187]
    AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30                  [SAS 188]

  The +0.30 expense load is the source-faithful quirk; if a future change drops
  it or alters the loss-ratio formula, this control fails and names the policy
  type.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with recomputed as (
    select
        policy_type,
        sum(ytd_earned_premium) as total_earned,
        sum(total_incurred) as total_incurred
    from {{ ref('int_policy_valuation') }}
    group by policy_type
),

mart as (
    select
        policy_type,
        total_earned,
        total_incurred,
        agg_loss_ratio,
        agg_combined_ratio
    from {{ ref('mart_loss_ratios') }}
)

select
    m.policy_type,
    m.total_earned,
    r.total_earned as expected_total_earned,
    m.agg_loss_ratio,
    m.agg_combined_ratio
from mart m
inner join recomputed r on m.policy_type = r.policy_type
where
    -- component totals must tie to a fresh aggregation of the intermediate
    abs(m.total_earned - r.total_earned) > 0.01
    or abs(m.total_incurred - r.total_incurred) > 0.01
    -- aggregate ratio formula parity (only meaningful when earned > 0)
    or (
        m.total_earned > 0
        and (
            abs(m.agg_loss_ratio - m.total_incurred / m.total_earned) > 0.0001
            or abs(
                m.agg_combined_ratio
                - (m.total_incurred / m.total_earned + 0.30)
            ) > 0.0001
        )
    )
