/*
  Reconciliation test: approved-amount control total.

  Verifies that the total approved_amount for auto-approved claims (APPR) in the
  model matches the expected sum re-derived directly from raw data using the SAS
  business rules. This catches any divergence in the deductible calculation or
  adjudication routing.

  The expected total is computed independently: raw claims → policy join → fraud
  risk derivation → adjudication routing → max(0, claimed - deductible) for APPR.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with raw_approved as (
    select
        sum(
            case
                -- Rule 1: LOW + <= 5000 + AUTO/HOME/RENT → APPR
                when fi.fraud_risk = 'LOW'
                     and c.claimed_amount <= 5000
                     and p.policy_type in ('AUTO', 'HOME', 'RENT')
                    then greatest(0, c.claimed_amount - p.deductible)
                -- Rule 2: LOW + <= 25% sum_insured + <= 50000 (not rule 1)
                when fi.fraud_risk = 'LOW'
                     and not (
                         c.claimed_amount <= 5000
                         and p.policy_type in ('AUTO', 'HOME', 'RENT')
                     )
                     and c.claimed_amount <= p.sum_insured * 0.25
                     and c.claimed_amount <= 50000
                    then greatest(0, c.claimed_amount - p.deductible)
                else null
            end
        ) as total_approved
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    left join (
        select
            claim_id,
            case
                when fraud_score >= 0.80 then 'HIGH'
                when fraud_score >= 0.50 then 'MEDIUM'
                else 'LOW'
            end as fraud_risk
        from {{ source('insurance_raw', 'fraud_indicators') }}
    ) fi
        on c.claim_id = fi.claim_id
    where p.policy_status = 'ACTIVE'
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
),

model_approved as (
    select sum(approved_amount) as total_approved
    from {{ ref('int_claims_adjudication') }}
    where adjudication_result = 'APPR'
)

select
    r.total_approved as expected_total_approved,
    m.total_approved as model_total_approved,
    abs(coalesce(m.total_approved, 0) - coalesce(r.total_approved, 0)) as difference
from raw_approved r
cross join model_approved m
where abs(
    coalesce(m.total_approved, 0) - coalesce(r.total_approved, 0)
) > 0.01
