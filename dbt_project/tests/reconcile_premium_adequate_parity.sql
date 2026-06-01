/*
  Reconciliation test: PREMIUM_ADEQUATE mapping parity.

  The SAS DATA step (policy_valuation.sas, Step 4) assigns PREMIUM_ADEQUATE
  with this logic:
      if COMBINED_RATIO = .       then PREMIUM_ADEQUATE = 'N';
      else if COMBINED_RATIO > 1.0 then PREMIUM_ADEQUATE = 'N';
      else PREMIUM_ADEQUATE = 'Y';

  This parity check re-derives the flag from the model's own COMBINED_RATIO
  and verifies every row matches. A mismatch means the CASE logic in
  int_policy_valuation has diverged from the SAS source.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with check_parity as (
    select
        policy_id,
        combined_ratio,
        premium_adequate,
        case
            when combined_ratio is null then 'N'
            when combined_ratio > 1.0 then 'N'
            else 'Y'
        end as expected_premium_adequate
    from {{ ref('int_policy_valuation') }}
)

select
    policy_id,
    combined_ratio,
    premium_adequate as actual,
    expected_premium_adequate as expected
from check_parity
where premium_adequate <> expected_premium_adequate
   or (premium_adequate is null and expected_premium_adequate is not null)
   or (premium_adequate is not null and expected_premium_adequate is null)
