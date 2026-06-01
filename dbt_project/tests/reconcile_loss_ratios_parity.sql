/*
  Reconciliation test: loss ratio mart parity.

  Verifies that each policy_type row in mart_loss_ratios has an
  agg_loss_ratio consistent with its own total_incurred / total_earned,
  and that the agg_combined_ratio equals agg_loss_ratio + 0.30 (the
  hard-coded expense load from SAS line 188).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select
    policy_type,
    total_earned,
    total_incurred,
    agg_loss_ratio,
    agg_combined_ratio,
    case
        when total_earned > 0
            then total_incurred / total_earned
        else null
    end as recomputed_loss_ratio,
    case
        when total_earned > 0
            then total_incurred / total_earned + 0.30
        else null
    end as recomputed_combined_ratio
from {{ ref('mart_loss_ratios') }}
where
    /* Loss ratio mismatch */
    abs(
        coalesce(agg_loss_ratio, -999)
        - coalesce(
            case when total_earned > 0 then total_incurred / total_earned else null end,
            -999
        )
    ) > 0.0001
    or
    /* Combined ratio mismatch */
    abs(
        coalesce(agg_combined_ratio, -999)
        - coalesce(
            case when total_earned > 0 then total_incurred / total_earned + 0.30 else null end,
            -999
        )
    ) > 0.0001
