/*
  Reconciliation test: claimed and approved control totals.

  Both expected totals are independently derived from raw claims, active
  policies, and claim-keyed fraud indicators using the SAS branch order.
*/
with valid_claims as (
    select
        c.claim_id,
        c.claimed_amount,
        p.policy_type,
        c.sum_insured,
        c.deductible,
        f.fraud_score
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

expected as (
    select
        sum(claimed_amount) as expected_claimed_amount,
        sum(
            case
                when fraud_score >= 80 then 0
                when (fraud_score is null or fraud_score < 50)
                     and claimed_amount <= 5000
                     and policy_type in ('AUTO', 'HOME', 'RENT')
                    then greatest(0, coalesce(claimed_amount - deductible, 0))
                when (fraud_score is null or fraud_score < 50)
                     and claimed_amount <= sum_insured * 0.25
                     and claimed_amount <= 50000
                    then greatest(0, coalesce(claimed_amount - deductible, 0))
                else null
            end
        ) as expected_approved_amount
    from valid_claims
),

actual as (
    select
        sum(claimed_amount) as actual_claimed_amount,
        sum(approved_amount) as actual_approved_amount
    from {{ ref('mart_claims_register') }}
)

select
    e.expected_claimed_amount,
    a.actual_claimed_amount,
    e.expected_approved_amount,
    a.actual_approved_amount
from expected e
cross join actual a
where not (
        e.expected_claimed_amount <=> a.actual_claimed_amount
    and e.expected_approved_amount <=> a.actual_approved_amount
)
