/*
  Reconciliation test: parity check — adjudication_result CASE mapping.

  The SAS source (claims_processing.sas, Step 3) defines four routing rules
  in priority order. This test re-derives the expected result from the input
  columns and verifies every row matches. Catches regressions where a rule
  boundary is accidentally changed (e.g. 5000 vs 50000, missing policy type).

  SAS adjudication rules (priority order):
    1. FRAUD_RISK = 'HIGH'                                       -> DENY
    2. FRAUD_RISK = 'LOW' AND claimed <= 5000
       AND policy_type IN ('AUTO','HOME','RENT')                 -> APPR
    3. FRAUD_RISK = 'LOW' AND claimed <= sum_insured * 0.25
       AND claimed <= 50000                                      -> APPR
    4. else                                                      -> PEND

  dbt singular test convention: returns rows on FAILURE.
*/
select
    claim_id,
    fraud_risk,
    claimed_amount,
    policy_type,
    sum_insured,
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
where adjudication_result <> (
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
    end
)
