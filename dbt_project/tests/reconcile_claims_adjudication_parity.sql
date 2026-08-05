/*
  Reconciliation test: per-row parity with the SAS Steps 2-3 CASE order.
*/
with expected as (
    select
        c.claim_id,
        case
            when f.fraud_score >= 80 then 'DENY'
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= 5000)
                 and p.policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                 and (c.claimed_amount is null or c.claimed_amount <= 50000)
                then 'APPR'
            else 'PEND'
        end as expected_result,
        case
            when f.fraud_score >= 80 then 0
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= 5000)
                 and p.policy_type in ('AUTO', 'HOME', 'RENT')
                then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                 and (c.claimed_amount is null or c.claimed_amount <= 50000)
                then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
            else null
        end as expected_approved_amount,
        case
            when f.fraud_score >= 80 then 'MANUAL_REVIEW'
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= 5000)
                 and p.policy_type in ('AUTO', 'HOME', 'RENT')
                then 'AUTO_ADJUDICATED'
            when (f.fraud_score is null or f.fraud_score < 50)
                 and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                 and (c.claimed_amount is null or c.claimed_amount <= 50000)
                then 'AUTO_ADJUDICATED'
            else 'MANUAL_REVIEW'
        end as expected_routing_target
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
       and p.policy_status = 'ACTIVE'
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on c.claim_id = f.claim_id
    where c.loss_date is not null
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and (
          c.claimed_amount is null
          or c.claimed_amount <= p.sum_insured
      )
),

actual as (
    select
        claim_id,
        adjudication_result,
        approved_amount,
        routing_target
    from {{ ref('int_claims_adjudication') }}
)

select
    e.claim_id,
    e.expected_result,
    a.adjudication_result,
    e.expected_approved_amount,
    a.approved_amount,
    e.expected_routing_target,
    a.routing_target
from expected e
inner join actual a
    on e.claim_id = a.claim_id
where e.expected_result <> a.adjudication_result
   or not (e.expected_approved_amount <=> a.approved_amount)
   or e.expected_routing_target <> a.routing_target
