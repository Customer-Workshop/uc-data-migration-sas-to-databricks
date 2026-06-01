/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 2: Fraud Screening,
                 Step 3: Auto-Adjudication Rules)

  SAS Original:
    PROC SQL join to TERA_DW.FRAUD_INDICATORS to attach a fraud score, then a
    DATA step with IF/THEN routing to AUTO_ADJUDICATED (APPR/DENY) and
    MANUAL_REVIEW (PEND).

  dbt Equivalent:
    The fraud-indicator join becomes a LEFT JOIN; the IF/THEN routing becomes a
    CASE expression. The SAS routing targets collapse to a single
    adjudication_result column (APPR / DENY / PEND).

  Note on synthetic data: the raw fraud_indicators feed in this demo carries a
  0..1 fraud_score keyed by claim_id; SAS keyed on (policy_id, claimant_id) with
  a 0..100 score. We scale to 0..100 and join on claim_id to keep parity of the
  HIGH/MEDIUM/LOW thresholds.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    select
        claim_id,
        fraud_score * 100 as fraud_score
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

scored as (
    select
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.claim_type,
        c.policy_type,
        c.claimed_amount,
        c.sum_insured,
        c.deductible,
        coalesce(f.fraud_score, 0) as fraud_score,
        -- SAS: FRAUD_RISK derived from FRAUD_SCORE thresholds
        case
            when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
            when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join fraud f
        on c.claim_id = f.claim_id
),

adjudicated as (
    select
        *,
        -- SAS Step 3: IF/THEN auto-adjudication routing -> single CASE
        case
            when fraud_risk = 'HIGH' then 'DENY'
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME')
                then 'APPR'
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then 'APPR'
            else 'PEND'
        end as adjudication_result
    from scored
)

select
    claim_id,
    policy_id,
    claimant_id,
    claim_type,
    policy_type,
    claimed_amount,
    sum_insured,
    deductible,
    fraud_score,
    fraud_risk,
    adjudication_result,
    -- SAS: APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE) when approved
    case
        when adjudication_result = 'APPR'
            then greatest(0, claimed_amount - deductible)
        when adjudication_result = 'DENY' then 0
        else null
    end as approved_amount,
    case adjudication_result
        when 'APPR' then 'Auto-approved'
        when 'DENY' then 'High fraud risk - SIU referral'
        else 'Routed to manual review'
    end as adjudication_reason,
    current_date() as processing_date
from adjudicated
