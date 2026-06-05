/*
  Reconciliation test: premium_adequate flag parity.

  Verifies the premium_adequate flag in int_policy_valuation matches the SAS
  rule value-for-value:
    - 'Y' if combined_ratio <= 1.0 AND combined_ratio IS NOT NULL
    - 'N' otherwise (combined_ratio IS NULL or > 1.0)

  combined_ratio = coalesce(total_incurred,0) / ytd_earned_premium + 0.30
  (only when ytd_earned_premium > 0, else NULL)

  This catches logic errors in the CASE mapping that aggregate checks miss.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with model_vals as (
    select
        policy_id,
        ytd_earned_premium,
        total_incurred,
        premium_adequate,
        combined_ratio
    from {{ ref('int_policy_valuation') }}
),

checked as (
    select
        policy_id,
        premium_adequate as actual_flag,
        case
            when ytd_earned_premium > 0
                and coalesce(total_incurred, 0) / ytd_earned_premium + 0.30 <= 1.0
                then 'Y'
            else 'N'
        end as expected_flag
    from model_vals
)

select
    policy_id,
    actual_flag,
    expected_flag
from checked
where actual_flag <> expected_flag
