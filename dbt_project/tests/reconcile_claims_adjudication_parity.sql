/*
  Reconciliation control: per-value parity of the auto-adjudication routing.

  This is the core source-of-truth control for claims_processing.sas Step 3.
  It independently recomputes, straight from the raw inputs, the SAS ordered
  IF/THEN/return chain for every in-scope claim and compares -- value for value
  -- the model's adjudication_result, approved_amount, and routing_target.

  It is the control that guards the source-faithful quirks:
    * DENY (high fraud) must route to MANUAL_REVIEW, not AUTO_ADJUDICATED. A
      conversion that "tidied" DENY into the auto-adjudicated set would surface
      here as a routing_target mismatch.
    * A missing fraud score routes as LOW (drives branches 2/3).
    * APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE) for approvals,
      0 for DENY, missing (NULL) for PEND.

  The recompute below is deliberately independent of the model (it reads raw
  claims/policies/fraud directly), so the two can only agree if the conversion
  reproduced the SAS rules exactly.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiry_date as expiration_date,
        sum_insured,
        deductible
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
),

valid_claims as (
    select
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.claimed_amount,
        p.policy_type,
        p.sum_insured,
        p.deductible
    from {{ source('insurance_raw', 'claims') }} c
    inner join active_policies p
        on c.policy_id = p.policy_id
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiration_date
      and c.claimed_amount <= p.sum_insured
),

scored as (
    select
        v.*,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from valid_claims v
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on v.policy_id = f.policy_id
        and v.claimant_id = f.claimant_id
),

expected as (
    select
        claim_id,
        case
            when fraud_risk = 'HIGH' then 1
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT') then 2
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000 then 3
            else 4
        end as branch,
        claimed_amount,
        deductible
    from scored
),

expected_final as (
    select
        claim_id,
        case branch when 1 then 'DENY' when 2 then 'APPR' when 3 then 'APPR' else 'PEND' end
            as expected_result,
        case branch
            when 1 then 0
            when 2 then greatest(0, claimed_amount - deductible)
            when 3 then greatest(0, claimed_amount - deductible)
            else cast(null as double)
        end as expected_approved_amount,
        case branch
            when 2 then 'AUTO_ADJUDICATED'
            when 3 then 'AUTO_ADJUDICATED'
            else 'MANUAL_REVIEW'
        end as expected_routing_target
    from expected
),

model as (
    select
        claim_id,
        adjudication_result,
        approved_amount,
        routing_target
    from {{ ref('int_claims_adjudication') }}
)

select
    m.claim_id,
    m.adjudication_result,
    e.expected_result,
    m.approved_amount,
    e.expected_approved_amount,
    m.routing_target,
    e.expected_routing_target
from model m
inner join expected_final e
    on m.claim_id = e.claim_id
where not (m.adjudication_result <=> e.expected_result)
   or not (m.approved_amount <=> e.expected_approved_amount)
   or not (m.routing_target <=> e.expected_routing_target)
