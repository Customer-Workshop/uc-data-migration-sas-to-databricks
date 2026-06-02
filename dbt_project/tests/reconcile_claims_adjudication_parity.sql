/*
  Reconciliation test: adjudication result parity by route.

  For each adjudication outcome (APPR, DENY, PEND), the count in the model must
  equal the count produced by independently re-deriving the SAS DATA step routing
  rules from raw data. This catches any divergence in the CASE evaluation order,
  threshold values, or join cardinality.

  SAS routing (claims_processing.sas Step 3):
    - HIGH fraud                                          → DENY
    - LOW fraud + claimed_amount <= 5000 + type in (AUTO,HOME,RENT) → APPR
    - LOW fraud + claimed_amount <= 25% sum_insured + <= 50000       → APPR
    - Everything else                                     → PEND

  The expected counts are derived end-to-end from raw tables (claims, policies,
  fraud_indicators) so the control is independent of the model logic.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with fraud_risk_scored as (
    select
        claim_id,
        case
            when fraud_score >= 0.80 then 'HIGH'
            when fraud_score >= 0.50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

raw_adjudicated as (
    select
        case
            when fi.fraud_risk = 'HIGH'
                then 'DENY'
            when fi.fraud_risk = 'LOW'
                 and c.claimed_amount <= 5000
                 and p.policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            when fi.fraud_risk = 'LOW'
                 and c.claimed_amount <= p.sum_insured * 0.25
                 and c.claimed_amount <= 50000
                then 'APPR'
            else 'PEND'
        end as expected_result
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    left join fraud_risk_scored fi
        on c.claim_id = fi.claim_id
    where p.policy_status = 'ACTIVE'
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
),

expected_counts as (
    select
        expected_result,
        count(*) as n
    from raw_adjudicated
    group by expected_result
),

model_counts as (
    select
        adjudication_result,
        count(*) as n
    from {{ ref('int_claims_adjudication') }}
    group by adjudication_result
)

select
    coalesce(e.expected_result, m.adjudication_result) as result_code,
    e.n as expected_count,
    m.n as model_count,
    coalesce(m.n, 0) - coalesce(e.n, 0) as difference
from expected_counts e
full outer join model_counts m
    on e.expected_result = m.adjudication_result
where coalesce(e.n, 0) <> coalesce(m.n, 0)
