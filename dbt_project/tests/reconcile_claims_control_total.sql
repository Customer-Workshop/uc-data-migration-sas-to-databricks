/*
  Reconciliation control: claims control totals.

  Two money totals must tie out between the raw source and the converted model,
  the way a SAS analyst would foot CLAIMS_REGISTER against the feed:

    1. total claimed   -- SUM(CLAIMED_AMOUNT) over the in-scope valid claims.
                          Proves no row/value loss or duplication in the carry
                          through staging -> adjudication.
    2. total approved  -- SUM(APPROVED_AMOUNT). Independently recomputed from
                          raw using the SAS rules (0 for DENY, max(0, claimed -
                          deductible) for APPR, NULL/0 for PEND), then compared
                          to the model's approved total.

  dbt singular test convention: FAILS if this query returns any rows. A penny
  tolerance absorbs float rounding only.
*/
with active_policies as (
    select
        policy_id,
        effective_date,
        expiry_date as expiration_date,
        sum_insured,
        deductible,
        policy_type
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

source_totals as (
    select
        round(sum(claimed_amount), 2) as total_claimed,
        round(sum(
            case
                when fraud_risk = 'HIGH' then 0
                when fraud_risk = 'LOW'
                     and claimed_amount <= 5000
                     and policy_type in ('AUTO', 'HOME', 'RENT')
                    then greatest(0, claimed_amount - deductible)
                when fraud_risk = 'LOW'
                     and claimed_amount <= sum_insured * 0.25
                     and claimed_amount <= 50000
                    then greatest(0, claimed_amount - deductible)
                else 0
            end
        ), 2) as total_approved
    from scored
),

model_totals as (
    select
        round(sum(claimed_amount), 2) as total_claimed,
        round(sum(coalesce(approved_amount, 0)), 2) as total_approved
    from {{ ref('int_claims_adjudication') }}
)

select
    s.total_claimed as source_total_claimed,
    m.total_claimed as model_total_claimed,
    s.total_approved as source_total_approved,
    m.total_approved as model_total_approved
from source_totals s
cross join model_totals m
where abs(s.total_claimed - m.total_claimed) > 0.01
   or abs(s.total_approved - m.total_approved) > 0.01
