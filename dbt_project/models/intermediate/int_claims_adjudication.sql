/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-4)

  SAS Original:
    Step 2 (PROC SQL):  left join WORK.CLAIMS_VALID to TERA_DW.FRAUD_INDICATORS,
                        derive FRAUD_RISK from FRAUD_SCORE thresholds.
    Step 3 (DATA step): auto-adjudication IF/THEN routing to AUTO_ADJUDICATED
                        (APPR / DENY) vs MANUAL_REVIEW (PEND), set APPROVED_AMOUNT.
    Step 4 (DATA step): CLAIMS_COMBINED = AUTO_ADJUDICATED + MANUAL_REVIEW,
                        CLAIM_STATUS = ADJUDICATION_RESULT, PROCESSING_DATE.

  dbt Equivalent:
    The SAS hash/PROC SQL fraud join becomes a LEFT JOIN; the DATA-step IF/THEN
    routing becomes SQL CASE expressions. The register (CLAIMS_REGISTER) is the
    union of AUTO_ADJUDICATED and MANUAL_REVIEW — i.e. one row per valid claim —
    so a single model with an adjudication_result column reproduces it.

  Source-faithfulness notes:
    - SAS joined fraud on (POLICY_ID, CLAIMANT_ID) and pulled INDICATOR_FLAGS.
      The migrated raw fraud_indicators is keyed by CLAIM_ID (the only available
      key) and has no INDICATOR_FLAGS column, so the join is on claim_id and the
      flags field is omitted.
    - FRAUD_RISK thresholds (>=80 HIGH, >=50 MEDIUM) are reproduced value-for-value
      from the SAS scorecard, which assumes a 0-100 score. The migrated
      fraud_indicators.fraud_score is on a 0-1 scale, so every claim currently
      falls to 'LOW'. This is reproduced as-is per the source-of-truth rule and
      flagged for a separate business decision -- NOT silently rescaled here.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- SAS Step 2: TERA_DW.FRAUD_INDICATORS (joined on claim_id; see note above)
fraud as (
    select
        claim_id,
        fraud_score,
        model_version
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

-- SAS Step 2: FRAUD_RISK CASE
screened as (
    select
        c.*,
        f.fraud_score,
        f.model_version,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join fraud f
        on c.claim_id = f.claim_id
),

-- SAS Step 3: Auto-Adjudication routing (AUTO_ADJUDICATED / MANUAL_REVIEW)
adjudicated as (
    select
        *,
        case
            -- SAS: if FRAUD_RISK='HIGH' -> DENY (SIU referral) -> MANUAL_REVIEW
            when fraud_risk = 'HIGH' then 'DENY'
            -- SAS: auto-approve low risk, small claim, AUTO/HOME/RENT
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            -- SAS: auto-approve standard claim within 25% of sum insured and <= 50000
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            -- SAS: everything else -> PEND -> MANUAL_REVIEW
            else 'PEND'
        end as adjudication_result
    from screened
),

final as (
    select
        *,

        -- SAS: ADJUDICATION_REASON assignment
        case
            when adjudication_result = 'DENY'
                then 'High fraud risk - SIU referral'
            when adjudication_result = 'APPR'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'Auto-approved: low risk, small claim'
            when adjudication_result = 'APPR'
                then 'Auto-approved: within 25% of sum insured'
            -- SAS: PEND reason = catx('; ', medium-risk, large-claim, exceeds-threshold)
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

        -- SAS: APPROVED_AMOUNT
        case
            -- SAS: DENY -> APPROVED_AMOUNT = 0
            when adjudication_result = 'DENY' then 0
            -- SAS: APPR -> max(0, CLAIMED_AMOUNT - DEDUCTIBLE)
            when adjudication_result = 'APPR'
                then greatest(0, claimed_amount - deductible)
            -- SAS: PEND -> APPROVED_AMOUNT = . (missing)
            else null
        end as approved_amount,

        -- SAS Step 4: CLAIM_STATUS = ADJUDICATION_RESULT
        adjudication_result as claim_status_adjudicated,
        current_date() as processing_date
    from adjudicated
)

select * from final
