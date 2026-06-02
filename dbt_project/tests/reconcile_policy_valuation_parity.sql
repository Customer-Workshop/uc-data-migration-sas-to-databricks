/*
  Reconciliation control: per-policy valuation formula parity.

  This is the source-parity gate for policy_valuation.sas Step 4. It asserts,
  row by row, that every derived metric stored in int_policy_valuation equals
  the value the SAS DATA step formulas produce from the same inputs. A mart
  total can tie out while an individual branch is wrong, so parity is checked
  per value, not in aggregate:

    LOSS_RATIO       = TOTAL_INCURRED / YTD_EARNED_PREMIUM     (else NULL)   [SAS 136-139]
    COMBINED_RATIO   = LOSS_RATIO + 0.30                       (else NULL)   [SAS 142-147]
    PREMIUM_ADEQUATE = 'N' if COMBINED_RATIO missing or > 1.0, else 'Y'     [SAS 149-152]
    IBNR_ESTIMATE    = max(0, YTD_EARNED_PREMIUM*0.15 - TOTAL_PAID)         [SAS 155]

  If a future "tidy-up" silently diverges from the source (e.g. flips the
  premium-adequacy comparison, drops the +0.30 expense load, or changes the
  IBNR factor) this control fails and names the offending policy -- the
  insurance analogue of the LOC risk-weight divergence in the playbook.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with model as (
    select
        policy_id,
        ytd_earned_premium,
        total_incurred,
        total_paid,
        loss_ratio,
        combined_ratio,
        premium_adequate,
        ibnr_estimate
    from {{ ref('int_policy_valuation') }}
),

expected as (
    select
        *,
        case when ytd_earned_premium > 0
            then total_incurred / ytd_earned_premium
        end as exp_loss_ratio,
        case when ytd_earned_premium > 0
            then total_incurred / ytd_earned_premium + 0.30
        end as exp_combined_ratio,
        case
            when ytd_earned_premium > 0
                 and (total_incurred / ytd_earned_premium + 0.30) <= 1.0
                then 'Y'
            else 'N'
        end as exp_premium_adequate,
        greatest(0, ytd_earned_premium * 0.15 - total_paid) as exp_ibnr_estimate
    from model
)

select
    policy_id,
    loss_ratio,
    exp_loss_ratio,
    combined_ratio,
    exp_combined_ratio,
    premium_adequate,
    exp_premium_adequate,
    ibnr_estimate,
    exp_ibnr_estimate
from expected
where
    -- loss ratio / combined ratio parity (NULL-safe, float tolerance)
    not (coalesce(abs(loss_ratio - exp_loss_ratio), 0) <= 0.0001
         and (loss_ratio is null) = (exp_loss_ratio is null))
    or not (coalesce(abs(combined_ratio - exp_combined_ratio), 0) <= 0.0001
            and (combined_ratio is null) = (exp_combined_ratio is null))
    -- premium adequacy branch parity
    or premium_adequate <> exp_premium_adequate
    -- IBNR parity
    or abs(ibnr_estimate - exp_ibnr_estimate) > 0.0001
