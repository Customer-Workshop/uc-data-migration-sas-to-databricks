/*
  Reconciliation test: per-claim parity of every CASE mapping against the SAS source.

  Parity is per-value, not aggregate: a mapping can produce a correct total while an
  individual branch is wrong (see the LOC risk-weight example in the playbook). This
  control independently re-derives, from the raw inputs, the two mappings that
  claims_processing.sas computes — FRAUD_RISK (Step 2) and ADJUDICATION_RESULT
  (Step 3) — using the SAS thresholds value-for-value, and compares them to what
  int_claims_adjudication produced for each claim. Any divergence (e.g. a changed
  fraud threshold, a dropped policy_type from the auto-approve list, a flipped
  comparison) surfaces as a returned row.

  SAS thresholds reproduced here (source of truth):
    FRAUD_RISK:  fraud_score >= 80 -> HIGH; >= 50 -> MEDIUM; else LOW
    ADJUDICATION:
      FRAUD_RISK = 'HIGH'                                                -> DENY
      FRAUD_RISK = 'LOW' and claimed <= 5000 and policy_type in (...)    -> APPR
      FRAUD_RISK = 'LOW' and claimed <= sum_insured*0.25 and <= 50000    -> APPR
      otherwise                                                          -> PEND

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_fraud as (
    select
        s.claim_id,
        s.claimed_amount,
        s.sum_insured,
        s.policy_type,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from {{ ref('stg_claims') }} s
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on s.claim_id = f.claim_id
),

expected as (
    select
        claim_id,
        fraud_risk,
        case
            when fraud_risk = 'HIGH' then 'DENY'
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            else 'PEND'
        end as adjudication_result
    from expected_fraud
),

model as (
    select
        claim_id,
        fraud_risk,
        adjudication_result
    from {{ ref('int_claims_adjudication') }}
)

select
    e.claim_id,
    e.fraud_risk as expected_fraud_risk,
    m.fraud_risk as actual_fraud_risk,
    e.adjudication_result as expected_adjudication,
    m.adjudication_result as actual_adjudication
from expected e
inner join model m
    on e.claim_id = m.claim_id
where e.fraud_risk <> m.fraud_risk
   or e.adjudication_result <> m.adjudication_result
