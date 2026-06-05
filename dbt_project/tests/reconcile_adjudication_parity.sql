/*
  Reconciliation test: adjudication parity — every adjudication_result in
  int_claims_adjudication matches the SAS routing rules exactly.

  The SAS DATA step (claims_processing.sas, Step 3) routes claims via sequential
  IF/THEN/RETURN:
    1. FRAUD_RISK='HIGH' → DENY
    2. FRAUD_RISK='LOW' AND claimed_amount<=5000 AND policy_type in (AUTO,HOME,RENT) → APPR
    3. FRAUD_RISK='LOW' AND claimed_amount<=sum_insured*0.25 AND claimed_amount<=50000 → APPR
    4. Everything else → PEND

  This test independently computes the expected adjudication_result from the
  underlying data and flags any row where the model disagrees. Each row is a
  parity violation.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with adjudicated as (
    select
        claim_id,
        fraud_risk,
        claimed_amount,
        policy_type,
        sum_insured,
        adjudication_result
    from {{ ref('int_claims_adjudication') }}
),

parity_check as (
    select
        claim_id,
        adjudication_result as actual_result,
        case
            when fraud_risk = 'HIGH' then 'DENY'
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            else 'PEND'
        end as expected_result
    from adjudicated
)

select
    claim_id,
    actual_result,
    expected_result
from parity_check
where actual_result <> expected_result
