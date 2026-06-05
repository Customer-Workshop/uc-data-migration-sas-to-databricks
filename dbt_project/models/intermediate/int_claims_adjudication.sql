/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  SAS Original:
    Step 2 — PROC SQL left join to TERA_DW.FRAUD_INDICATORS on
             POLICY_ID + CLAIMANT_ID, CASE for fraud_risk thresholds.
    Step 3 — DATA step with sequential IF/THEN/RETURN for auto-adjudication:
             1. HIGH fraud → DENY (approved_amount=0)
             2. LOW fraud AND claimed_amount<=5000 AND policy_type in (AUTO,HOME,RENT) → APPR
             3. LOW fraud AND claimed_amount<=sum_insured*0.25 AND claimed_amount<=50000 → APPR
             4. Everything else → PEND (manual review)
             Order matters: first matching rule wins (SAS RETURN exits iteration).

  dbt Equivalent:
    LEFT JOIN replaces SAS left join to fraud_indicators.
    Nested CASE WHEN reproduces the SAS sequential IF/THEN/RETURN routing
    exactly — first matching branch wins, remainder falls to PEND.
*/

with stg as (
    select * from {{ ref('stg_claims') }}
),

fraud_screening as (
    -- SAS Step 2: PROC SQL left join to TERA_DW.FRAUD_INDICATORS
    select
        s.*,
        f.fraud_score,
        f.indicator_flags,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from stg s
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on s.policy_id = f.policy_id
        and s.claimant_id = f.claimant_id
),

adjudicated as (
    -- SAS Step 3: DATA step sequential IF/THEN/RETURN auto-adjudication
    -- Order of CASE branches mirrors SAS RETURN order exactly.
    select
        *,

        case
            -- Rule 1: HIGH fraud → DENY (SAS: output WORK.MANUAL_REVIEW; return;)
            when fraud_risk = 'HIGH' then 'DENY'
            -- Rule 2: LOW risk, small claim, eligible policy type → APPR
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            -- Rule 3: LOW risk, within 25% of sum_insured and <=50000 → APPR
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            -- Rule 4: Everything else → PEND (SAS: output WORK.MANUAL_REVIEW;)
            else 'PEND'
        end as adjudication_result,

        case
            when fraud_risk = 'HIGH' then 'High fraud risk - SIU referral'
            when fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'Auto-approved: low risk, small claim'
            when fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'Auto-approved: within 25% of sum insured'
            else concat_ws('; ',
                nullif(case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end, ''),
                nullif(case when claimed_amount > 50000 then 'Large claim' end, ''),
                nullif(case when claimed_amount > sum_insured * 0.25 then 'Exceeds 25% threshold' end, '')
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
            else cast(null as double)
        end as approved_amount,

        current_date() as processing_date

    from fraud_screening
)

select * from adjudicated
