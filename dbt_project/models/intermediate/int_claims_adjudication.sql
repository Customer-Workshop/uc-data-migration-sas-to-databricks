/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  SAS Original:
    Step 2 — PROC SQL joining validated claims to TERA_DW.FRAUD_INDICATORS,
    deriving FRAUD_RISK level (HIGH >= 80, MEDIUM >= 50, LOW < 50).
    Step 3 — DATA step auto-adjudication rules routing claims to
    WORK.AUTO_ADJUDICATED (APPR/DENY) or WORK.MANUAL_REVIEW (PEND).

  dbt Equivalent:
    Broadcast JOIN replaces PROC SQL fraud lookup.
    SQL CASE expressions replace DATA step IF/THEN routing logic.
    All routing paths (DENY, APPR, PEND) are captured in a single model.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- SAS: PROC SQL join to TERA_DW.FRAUD_INDICATORS
fraud_check as (
    select
        c.*,
        f.fraud_score,
        f.indicator_flags,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on c.policy_id = f.policy_id
        and c.claimant_id = f.claimant_id
),

-- SAS: DATA step auto-adjudication (Step 3)
adjudicated as (
    select
        *,
        case
            -- Auto-deny: high fraud risk → SIU referral
            when fraud_risk = 'HIGH'
                then 'DENY'
            -- Auto-approve: low risk, small claim on AUTO/HOME/RENT
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            -- Auto-approve: low risk, within 25% of sum insured and <= 50K
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then 'APPR'
            -- Everything else → manual review
            else 'PEND'
        end as adjudication_result,

        case
            when fraud_risk = 'HIGH'
                then 'High fraud risk - SIU referral'
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'Auto-approved: low risk, small claim'
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then 'Auto-approved: within 25% of sum insured'
            else concat_ws('; ',
                nullif(case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end, ''),
                nullif(case when claimed_amount > 50000 then 'Large claim' end, ''),
                nullif(case when claimed_amount > sum_insured * 0.25 then 'Exceeds 25% threshold' end, '')
            )
        end as adjudication_reason,

        case
            when fraud_risk = 'HIGH'
                then 0
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then greatest(0, claimed_amount - deductible)
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then greatest(0, claimed_amount - deductible)
            else null
        end as approved_amount,

        current_date() as processing_date

    from fraud_check
)

select * from adjudicated
