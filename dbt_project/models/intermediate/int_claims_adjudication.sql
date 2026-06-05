/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-4)

  SAS Original:
    Step 2: PROC SQL fraud screening join to TERA_DW.FRAUD_INDICATORS
    Step 3: DATA step auto-adjudication with IF/THEN routing to
            WORK.AUTO_ADJUDICATED / WORK.MANUAL_REVIEW
    Step 4: SET + PROC APPEND to STG_INS.CLAIMS_REGISTER

  dbt Equivalent:
    LEFT JOIN replaces PROC SQL fraud check.
    CASE expressions replace DATA step adjudication routing.
    SAS hash object → broadcast JOIN pattern.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- SAS Step 2: PROC SQL creating WORK.FRAUD_CHECK
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

-- SAS Steps 3-4: Auto-adjudication routing logic
adjudicated as (
    select
        *,

        -- SAS: IF/THEN routing to AUTO_ADJUDICATED or MANUAL_REVIEW
        case
            when fraud_risk = 'HIGH'
                then 'DENY'
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then 'APPR'
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
                case when fraud_risk = 'MEDIUM'
                    then 'Medium fraud risk' end,
                case when claimed_amount > 50000
                    then 'Large claim' end,
                case when claimed_amount > sum_insured * 0.25
                    then 'Exceeds 25% threshold' end
            )
        end as adjudication_reason,

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
            else null
        end as approved_amount

    from fraud_check
)

select * from adjudicated
