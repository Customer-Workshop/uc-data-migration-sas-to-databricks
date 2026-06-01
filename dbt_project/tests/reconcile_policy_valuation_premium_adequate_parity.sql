/*
  Reconciliation test: PREMIUM_ADEQUATE mapping parity.

  The SAS program (policy_valuation.sas, Step 4) assigns PREMIUM_ADEQUATE
  via this exact logic:
      if COMBINED_RATIO = .           then PREMIUM_ADEQUATE = 'N';
      else if COMBINED_RATIO > 1.0    then PREMIUM_ADEQUATE = 'N';
      else                                 PREMIUM_ADEQUATE = 'Y';

  This parity check recomputes the flag from the intermediate model's own
  combined_ratio column and compares it to the stored premium_adequate value.
  Any row where the stored value does not match the recomputed value is a
  mapping divergence.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select
    policy_id,
    combined_ratio,
    premium_adequate as stored_value,
    case
        when combined_ratio is null then 'N'
        when combined_ratio > 1.0   then 'N'
        else 'Y'
    end as recomputed_value
from {{ ref('int_policy_valuation') }}
where premium_adequate <>
    case
        when combined_ratio is null then 'N'
        when combined_ratio > 1.0   then 'N'
        else 'Y'
    end
