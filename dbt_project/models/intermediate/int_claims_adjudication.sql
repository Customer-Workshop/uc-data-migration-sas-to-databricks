/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  SAS Original:
    Step 2 — Fraud Screening:
      PROC SQL LEFT JOIN to TERA_DW.FRAUD_INDICATORS on POLICY_ID + CLAIMANT_ID.
      CASE assigns FRAUD_RISK: HIGH (>=80), MEDIUM (>=50), LOW (<50).

    Step 3 — Auto-Adjudication Rules (DATA step with IF/THEN routing):
      1. FRAUD_RISK = 'HIGH' -> DENY, route to MANUAL_REVIEW
      2. FRAUD_RISK = 'LOW' AND claimed <= 5000 AND policy_type IN (AUTO,HOME,RENT)
         -> APPR, approved_amount = max(0, claimed - deductible)
      3. FRAUD_RISK = 'LOW' AND claimed <= sum_insured*0.25 AND claimed <= 50000
         -> APPR, approved_amount = max(0, claimed - deductible)
      4. Everything else -> PEND, route to MANUAL_REVIEW

  dbt Equivalent:
    LEFT JOIN replaces PROC SQL join to fraud_indicators.
    Nested CASE WHEN reproduces the IF/THEN/ELSE priority chain exactly.
    Both AUTO_ADJUDICATED and MANUAL_REVIEW outputs are combined (matching
    WORK.CLAIMS_COMBINED in Step 4). The PySpark job handles the multi-output
    routing to CLAIMS_REGISTER, CLAIMS_REVIEW_QUEUE, and FRAUD_ALERTS.

  Quirks reproduced from SAS source (flagged, not fixed):
    - QUIRK: policy_type 'RENT' is referenced in the auto-approve rule but
      the policies source only contains types: AUTO, HOME, LIFE, HEALTH,
      COMMERCIAL. 'RENT' will never match in practice. Source-faithful.
    - QUIRK: The second auto-approve rule (<=25% of sum_insured AND <=50000)
      is only reachable when FRAUD_RISK='LOW'. A LOW-risk claim between 5001
      and min(sum_insured*0.25, 50000) hits rule 3. Claims >50000 or >25%
      always go to PEND regardless of risk (unless HIGH). Source-faithful.
    - QUIRK: When fraud_indicators has no matching row (LEFT JOIN produces NULL),
      FRAUD_SCORE is NULL and the CASE maps it to 'LOW'. In SAS, rc=0 from
      h_pol.find() means "not found" — but the fraud join is a separate SQL step
      where a non-match yields NULL. The ELSE 'LOW' handles this identically to
      the SAS original. Source-faithful.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    -- SAS: TERA_DW.FRAUD_INDICATORS
    select
        policy_id,
        claimant_id,
        fraud_score,
        indicator_flags
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

fraud_screened as (
    -- Step 2: Fraud Screening (PROC SQL LEFT JOIN)
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
    left join fraud f
        on c.policy_id = f.policy_id
        and c.claimant_id = f.claimant_id
),

adjudicated as (
    -- Step 3: Auto-Adjudication Rules (DATA step IF/THEN/ELSE chain)
    select
        *,
        case
            -- Rule 1: Auto-deny high fraud risk
            when fraud_risk = 'HIGH' then 'DENY'
            -- Rule 2: Auto-approve low risk, small claim, eligible policy type
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
            then 'APPR'
            -- Rule 3: Auto-approve low risk, within 25% of sum insured and <= 50000
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
            then 'APPR'
            -- Rule 4: Everything else -> manual review
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
                nullif(
                    case when claimed_amount > sum_insured * 0.25
                    then 'Exceeds 25% threshold' end, ''
                )
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

    from fraud_screened
)

select * from adjudicated
