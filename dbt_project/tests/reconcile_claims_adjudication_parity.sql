/*
  Reconciliation test: adjudication-result parity.

  Verifies that the CASE WHEN routing in int_claims_adjudication exactly
  reproduces the SAS IF/THEN/ELSE routing logic branch-for-branch. Each
  row's adjudication_result is compared to the expected result derived
  independently from the SAS conditions:

    FRAUD_RISK = 'HIGH'                                            → DENY
    FRAUD_RISK = 'LOW'  AND claimed <= 5000
                        AND policy_type IN ('AUTO','HOME','RENT')  → APPR
    FRAUD_RISK = 'LOW'  AND claimed <= sum_insured * 0.25
                        AND claimed <= 50000                       → APPR
    Everything else                                                → PEND

  -- SAS: thresholds 5000, 50000, 0.25 are source-faithful

  Any row whose model result differs from the expected result is a
  divergence. This is a per-row parity check, not an aggregate — it
  catches branch-level errors that a total might mask.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with adjudicated as (
    select
        claim_id,
        fraud_risk,
        policy_type,
        claimed_amount,
        sum_insured,
        adjudication_result,
        -- Reproduce the SAS routing logic independently
        case
            when fraud_risk = 'HIGH'
                then 'DENY'
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
    fraud_risk,
    policy_type,
    claimed_amount,
    sum_insured,
    adjudication_result as actual_result,
    expected_result
from adjudicated
where adjudication_result <> expected_result
