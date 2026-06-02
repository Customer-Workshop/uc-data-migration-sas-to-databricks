/*
  Reconciliation test: approved-amount control total.

  The SUM of approved_amount in the model for auto-approved claims (APPR) must
  equal the control total computed by independently applying the SAS formula:
    max(0, claimed_amount - deductible)
  to the auto-approved population identified by the SAS routing rules.

  The expected total is derived end-to-end from raw tables — claims joined to
  active policies (Step 1 scope), fraud risk bucketed (Step 2), then routed
  through auto-adjudication (Step 3) — so the control is fully independent of
  model logic.

  Zero tolerance: any non-zero difference means the deductible calculation or
  adjudication routing has diverged from the SAS source.

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

raw_approved as (
    select
        sum(
            case
                -- Rule 1: LOW + <= 5000 + AUTO/HOME/RENT → APPR
                when coalesce(fi.fraud_risk, 'LOW') = 'LOW'
                     and c.claimed_amount <= 5000
                     and p.policy_type in ('AUTO', 'HOME', 'RENT')
                    then greatest(0, c.claimed_amount - p.deductible)
                -- Rule 2: LOW + <= 25% sum_insured + <= 50000 (not caught by rule 1)
                when coalesce(fi.fraud_risk, 'LOW') = 'LOW'
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
    left join fraud_risk_scored fi
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
    coalesce(m.total_approved, 0) - coalesce(r.total_approved, 0) as difference
from raw_approved r
cross join model_approved m
where abs(
    coalesce(m.total_approved, 0) - coalesce(r.total_approved, 0)
) > 0
