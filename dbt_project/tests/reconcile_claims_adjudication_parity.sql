/*
  Reconciliation test: adjudication result parity.

  Verifies the adjudication routing matches the SAS rules applied to
  the same data. Re-derives the expected result from stg_claims +
  fraud_indicators and compares to int_claims_adjudication.

  The SAS routing rules (claims_processing.sas, Step 3):
    HIGH fraud_risk → DENY
    LOW + claimed <= 5000 + policy_type in (AUTO,HOME,RENT) → APPR
    LOW + claimed <= 25% sum_insured + claimed <= 50000 → APPR
    else → PEND

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected as (
    select
        c.claim_id,
        case
            when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
            when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
            else 'LOW'
        end as expected_fraud_risk,
        case
            when (case
                      when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
                      when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
                      else 'LOW'
                  end) = 'HIGH'
            then 'DENY'
            when (case
                      when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
                      when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
                      else 'LOW'
                  end) = 'LOW'
                 and c.claimed_amount <= 5000
                 and c.policy_type in ('AUTO', 'HOME', 'RENT')
            then 'APPR'
            when (case
                      when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
                      when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
                      else 'LOW'
                  end) = 'LOW'
                 and c.claimed_amount <= c.sum_insured * 0.25
                 and c.claimed_amount <= 50000
            then 'APPR'
            else 'PEND'
        end as expected_result
    from {{ ref('stg_claims') }} c
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on c.claim_id = f.claim_id
),

actual as (
    select
        claim_id,
        adjudication_result
    from {{ ref('int_claims_adjudication') }}
)

select
    a.claim_id,
    e.expected_result,
    a.adjudication_result as actual_result
from expected e
inner join actual a on e.claim_id = a.claim_id
where e.expected_result <> a.adjudication_result
