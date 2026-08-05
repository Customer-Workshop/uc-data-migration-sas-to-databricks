/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  Source mappings and intentional divergences:
    - TERA_DW.FRAUD_INDICATORS is keyed by POLICY_ID and CLAIMANT_ID in SAS.
      insurance_raw.fraud_indicators is keyed only by claim_id, so this model
      joins on claim_id. The composite-key-to-claim-key change can alter
      duplicate/fan-out behavior and is preserved as a source divergence.
    - INDICATOR_FLAGS is absent from the raw table, so the SAS alert-reason
      component is omitted rather than replaced with an invented value.
    - SAS thresholds remain literal (>= 80 HIGH, >= 50 MEDIUM) even though
      the synthetic seed currently generates fraud_score values from 0 to 1.
      This is a source-data fidelity gap, not a reason to rescale the score.
    - SAS HIGH fraud claims are DENY but are output to MANUAL_REVIEW. This
      source quirk is represented by routing_target.
    - SAS max(0, x) ignores missing arguments. Coalescing the arithmetic to
      zero preserves a missing deductible or claimed amount as approved 0.
    - A missing FRAUD_SCORE fails both SAS comparisons and falls through to
      LOW, which is the explicit ELSE branch below.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    select
        claim_id,
        fraud_score,
        model_version
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

fraud_check as (
    select
        c.*,
        f.fraud_score,
        f.model_version,
        {{ format_fraud_risk('f.fraud_score') }} as fraud_risk
    from claims c
    left join fraud f
        on c.claim_id = f.claim_id
),

adjudicated as (
    select
        *,
        case
            when fraud_risk = 'HIGH' then 'DENY'
            when fraud_risk = 'LOW'
                 -- SAS missing numerics sort below every number.
                 and (claimed_amount is null or claimed_amount <= 5000)
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            when fraud_risk = 'LOW'
                 and (claimed_amount is null or claimed_amount <= sum_insured * 0.25)
                 and (claimed_amount is null or claimed_amount <= 50000)
                then 'APPR'
            else 'PEND'
        end as adjudication_result,
        case
            when fraud_risk = 'HIGH'
                then 'High fraud risk - SIU referral'
            when fraud_risk = 'LOW'
                 and (claimed_amount is null or claimed_amount <= 5000)
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'Auto-approved: low risk, small claim'
            when fraud_risk = 'LOW'
                 and (claimed_amount is null or claimed_amount <= sum_insured * 0.25)
                 and (claimed_amount is null or claimed_amount <= 50000)
                then 'Auto-approved: within 25% of sum insured'
            else concat_ws(
                '; ',
                case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end,
                case when claimed_amount > 50000 then 'Large claim' end,
                case
                    when claimed_amount > sum_insured * 0.25
                        then 'Exceeds 25% threshold'
                end
            )
        end as adjudication_reason,
        case
            when fraud_risk = 'HIGH' then 0
            when fraud_risk = 'LOW'
                 and (claimed_amount is null or claimed_amount <= 5000)
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then greatest(0, coalesce(claimed_amount - deductible, 0))
            when fraud_risk = 'LOW'
                 and (claimed_amount is null or claimed_amount <= sum_insured * 0.25)
                 and (claimed_amount is null or claimed_amount <= 50000)
                then greatest(0, coalesce(claimed_amount - deductible, 0))
            else null
        end as approved_amount
    from fraud_check
)

select
    *,
    case
        when adjudication_result = 'APPR' then 'AUTO_ADJUDICATED'
        else 'MANUAL_REVIEW'
    end as routing_target
from adjudicated
