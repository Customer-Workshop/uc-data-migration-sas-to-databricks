/*
  Reconciliation test: adjudication result parity.

  Verifies the adjudication routing in int_claims_adjudication matches
  the SAS rules. Re-derives the expected result from the columns already
  present in the materialized table (fraud_risk, claimed_amount,
  policy_type, sum_insured) so the test is self-contained and independent
  of raw data freshness.

  The SAS routing rules (claims_processing.sas, Step 3):
    HIGH fraud_risk → DENY
    LOW + claimed <= 5000 + policy_type in (AUTO,HOME,RENT) → APPR
    LOW + claimed <= 25% sum_insured + claimed <= 50000 → APPR
    else → PEND

  dbt singular test convention: FAILS if this query returns any rows.
*/
with parity_check as (
    select
        claim_id,
        adjudication_result,
        case
            when fraud_risk = 'HIGH' then 'DENY'
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
            then 'APPR'
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
            then 'APPR'
            else 'PEND'
        end as expected_result
    from {{ ref('int_claims_adjudication') }}
)

select
    claim_id,
    expected_result,
    adjudication_result as actual_result
from parity_check
where expected_result <> adjudication_result
