/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  SAS Original:
    Step 2 — Fraud Screening:
      PROC SQL LEFT JOIN to TERA_DW.FRAUD_INDICATORS on POLICY_ID, CLAIMANT_ID.
      CASE on FRAUD_SCORE: >= 80 → HIGH, >= 50 → MEDIUM, else → LOW.
    Step 3 — Auto-Adjudication:
      DATA step IF/THEN routing:
        HIGH → DENY (approved_amount = 0)
        LOW + claimed <= 5000 + policy_type in (AUTO,HOME,RENT) → APPR
        LOW + claimed <= 25% sum_insured + claimed <= 50000 → APPR
        else → PEND

  dbt Equivalent:
    LEFT JOIN to fraud_indicators (joined on claim_id — see parity note).
    CASE WHEN reproduces the SAS IF/THEN routing exactly.

  Source-parity notes:
    1. SAS joined fraud_indicators on (POLICY_ID, CLAIMANT_ID); the Databricks
       seed table keys fraud_indicators on claim_id. Join key adapted to match
       the available schema; logic preserved.
    2. FRAUD_SCORE SCALE DIVERGENCE: The SAS code uses thresholds >= 80 and
       >= 50, implying a 0-100 scale. The seed data generates fraud_score as
       random.uniform(0, 1), a 0-1 scale. This means ALL seed claims will
       classify as LOW risk (max score ~1.0 << 50). Implemented per SAS
       thresholds exactly as specified; the reconciliation will surface this
       as 100% LOW / 0% HIGH / 0% MEDIUM. This is a known source-parity
       divergence, not a conversion bug.
    3. SAS checks POLICY_TYPE in ('AUTO','HOME','RENT'). The seed data does
       not include 'RENT' as a policy type. Condition preserved source-faithful.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    select * from {{ source('insurance_raw', 'fraud_indicators') }}
),

fraud_screened as (
    select
        c.*,
        f.fraud_score,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join fraud f
        on c.claim_id = f.claim_id
),

adjudicated as (
    select
        claim_id,
        policy_id,
        claimant_id,
        claim_type,
        claim_status,
        claimed_amount,
        loss_date,
        reported_date,
        policy_type,
        effective_date,
        expiry_date,
        sum_insured,
        deductible,
        fraud_score,
        fraud_risk,
        case
            -- SAS: HIGH → DENY
            when fraud_risk = 'HIGH' then 'DENY'
            -- SAS: LOW + small claim + specific policy types → APPR
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
            then 'APPR'
            -- SAS: LOW + within 25% of sum insured + <= 50000 → APPR
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
            then 'APPR'
            -- SAS: everything else → PEND
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
            else coalesce(
                nullif(
                    concat_ws('; ',
                        case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end,
                        case when claimed_amount > 50000 then 'Large claim' end,
                        case when claimed_amount > sum_insured * 0.25
                             then 'Exceeds 25% threshold' end
                    ), ''
                ), 'Manual review required'
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
        end as approved_amount,
        current_date() as processing_date,
        current_timestamp() as load_timestamp
    from fraud_screened
)

select * from adjudicated
