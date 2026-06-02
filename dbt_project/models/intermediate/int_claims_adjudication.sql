/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-4)

  SAS Original:
    Step 2 — PROC SQL left join to TERA_DW.FRAUD_INDICATORS; CASE for
             FRAUD_RISK (HIGH >= 80, MEDIUM >= 50, else LOW).
    Step 3 — DATA step IF/THEN/ELSE routing claims to AUTO_ADJUDICATED
             or MANUAL_REVIEW with thresholds 5000, 50000, 0.25.
    Step 4 — PROC APPEND combining AUTO_ADJUDICATED + MANUAL_REVIEW
             into STG_INS.CLAIMS_REGISTER.

  dbt Equivalent:
    LEFT JOIN replaces PROC SQL join; CASE WHEN chain replaces DATA
    step IF/THEN/ELSE routing; single model replaces the two-output
    DATA step + PROC APPEND combination.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    -- SAS: from TERA_DW.FRAUD_INDICATORS f
    select * from {{ source('insurance_raw', 'fraud_indicators') }}
),

-- SAS: Step 2 — PROC SQL fraud screening
fraud_screened as (
    select
        c.*,
        f.fraud_score,
        f.indicator_flags,
        -- SAS: case when f.FRAUD_SCORE >= 80 then 'HIGH'
        --           when f.FRAUD_SCORE >= 50 then 'MEDIUM'
        --           else 'LOW' end as FRAUD_RISK
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join fraud f
        on c.policy_id = f.policy_id
        and c.claimant_id = f.claimant_id
),

-- SAS: Step 3 — DATA step auto-adjudication IF/THEN/ELSE routing
-- Order-faithful reproduction: each branch mirrors the SAS IF block + RETURN
adjudicated as (
    select
        *,

        -- SAS: adjudication_result routing (IF/THEN/ELSE with RETURN)
        case
            -- SAS: if FRAUD_RISK = 'HIGH' then ADJUDICATION_RESULT = 'DENY'
            when fraud_risk = 'HIGH'
                then 'DENY'
            -- SAS: if FRAUD_RISK = 'LOW' and CLAIMED_AMOUNT <= 5000
            --      and POLICY_TYPE in ('AUTO','HOME','RENT') then 'APPR'
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            -- SAS: if FRAUD_RISK = 'LOW' and CLAIMED_AMOUNT <= SUM_INSURED * 0.25
            --      and CLAIMED_AMOUNT <= 50000 then 'APPR'
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000
                then 'APPR'
            -- SAS: everything else → PEND (output MANUAL_REVIEW)
            else 'PEND'
        end as adjudication_result,

        -- SAS: ADJUDICATION_REASON per branch
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
            -- SAS: catx('; ', ifc(FRAUD_RISK='MEDIUM', ...), ifc(...), ifc(...))
            else concat_ws('; ',
                case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end,
                case when claimed_amount > 50000 then 'Large claim' end,
                case when claimed_amount > sum_insured * 0.25 then 'Exceeds 25% threshold' end
            )
        end as adjudication_reason,

        -- SAS: APPROVED_AMOUNT per branch
        case
            -- SAS: APPROVED_AMOUNT = 0 (DENY)
            when fraud_risk = 'HIGH'
                then cast(0 as double)
            -- SAS: APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE)
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT')
                then greatest(0, claimed_amount - deductible)
            -- SAS: APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE)
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000
                then greatest(0, claimed_amount - deductible)
            -- SAS: APPROVED_AMOUNT = . (missing → NULL)
            else null
        end as approved_amount,

        -- SAS: Step 4 — PROCESSING_DATE = "&proc_date"d
        current_date() as processing_date

    from fraud_screened
)

select * from adjudicated
