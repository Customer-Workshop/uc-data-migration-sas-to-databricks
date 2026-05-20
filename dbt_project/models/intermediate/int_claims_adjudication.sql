/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-4)

  SAS Original:
    Step 2: PROC SQL join to TERA_DW.FRAUD_INDICATORS for fraud screening
    Step 3: DATA step IF/THEN routing for auto-adjudication:
            - HIGH fraud → DENY + SIU referral
            - LOW fraud + small claim + eligible type → APPR
            - LOW fraud + within 25% sum insured + ≤ 50k → APPR
            - Everything else → PEND (manual review)
    Step 4: SET combines AUTO_ADJUDICATED + MANUAL_REVIEW

  dbt Equivalent:
    LEFT JOIN replaces PROC SQL fraud check
    SQL CASE replaces DATA step IF/THEN routing logic
    Single model replaces the split/combine pattern
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

-- SAS Step 2: PROC SQL join to TERA_DW.FRAUD_INDICATORS
fraud_screened as (
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

-- SAS Step 3: DATA step auto-adjudication routing
adjudicated as (
    select
        *,

        -- SAS: IF/THEN adjudication result assignment
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

        -- SAS: adjudication reason
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
                nullif(case when claimed_amount > sum_insured * 0.25 then 'Exceeds 25% threshold' end, ''))
        end as adjudication_reason,

        -- SAS: APPROVED_AMOUNT calculation
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

        current_date() as processing_date,
        current_timestamp() as load_timestamp

    from fraud_screened
)

select * from adjudicated
