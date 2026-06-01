/*
  Reconciliation test: loss ratio / combined ratio formula parity.

  Verifies the mart-level ratios match the SAS formulas exactly:
    AGG_LOSS_RATIO     = TOTAL_INCURRED / TOTAL_EARNED
    AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30   (hard-coded expense load)

  Any row where the formula does not hold (within floating-point tolerance)
  indicates the conversion has diverged from the SAS source.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    policy_type,
    agg_loss_ratio,
    agg_combined_ratio,
    total_incurred,
    total_earned,
    /* Expected values from SAS formulas */
    case when total_earned > 0
        then total_incurred / total_earned
        else null
    end as expected_loss_ratio,
    case when total_earned > 0
        then total_incurred / total_earned + 0.30
        else null
    end as expected_combined_ratio
from {{ ref('mart_loss_ratios') }}
where total_earned > 0
  and (
    abs(agg_loss_ratio - total_incurred / total_earned) > 0.0001
    or abs(agg_combined_ratio - (total_incurred / total_earned + 0.30)) > 0.0001
  )
