/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-3)

  SAS Original:
    Step 2: PROC SQL left join to TERA_DW.FRAUD_INDICATORS on POLICY_ID/CLAIMANT_ID,
            CASE assigning FRAUD_RISK (HIGH >= 80, MEDIUM >= 50, else LOW).
    Step 3: DATA step IF/THEN routing:
            HIGH fraud → DENY (output MANUAL_REVIEW, SIU referral)
            LOW + amount <= 5000 + type in (AUTO,HOME,RENT) → APPR (auto-adjudicated)
            LOW + amount <= 25% sum_insured + amount <= 50000 → APPR (auto-adjudicated)
            Everything else → PEND (output MANUAL_REVIEW)

  dbt Equivalent:
    LEFT JOIN replaces SAS PROC SQL fraud join.
    CASE expressions replace DATA step IF/THEN/RETURN routing.
    Evaluation order in CASE mirrors SAS DATA step sequential returns.

  Source-faithful notes:
    - Fraud join key: SAS joined on POLICY_ID+CLAIMANT_ID (Teradata schema);
      Databricks raw data keys fraud_indicators by claim_id.
    - Fraud score scale: SAS thresholds were 80/50 on a 0-100 scale; raw data
      stores scores in 0-1 range. Thresholds scaled to 0.80/0.50 accordingly.
    - POLICY_TYPE check includes 'RENT' per SAS source, though no current
      policies carry that type — reproduced faithfully, not removed.
    - SAS INDICATOR_FLAGS column absent from Databricks raw schema; omitted.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    select * from {{ source('insurance_raw', 'fraud_indicators') }}
),

-- Step 2: Fraud Screening (SAS PROC SQL)
fraud_screened as (
    select
        c.*,
        f.fraud_score,
        -- SAS: case when f.FRAUD_SCORE >= 80 then 'HIGH' ...
        -- Source-faithful: thresholds scaled from 0-100 to 0-1
        case
            when f.fraud_score >= 0.80 then 'HIGH'
            when f.fraud_score >= 0.50 then 'MEDIUM'
            else 'LOW'
        end as fraud_risk
    from claims c
    left join fraud f
        on c.claim_id = f.claim_id
),

-- Step 3: Auto-Adjudication Rules (SAS DATA step IF/THEN/RETURN)
adjudicated as (
    select
        *,

        -- Adjudication result: CASE evaluated in SAS DATA step order
        case
            -- SAS: if FRAUD_RISK = 'HIGH' then DENY; output MANUAL_REVIEW; return;
            when fraud_risk = 'HIGH'
                then 'DENY'
            -- SAS: if FRAUD_RISK = 'LOW' and CLAIMED_AMOUNT <= 5000
            --      and POLICY_TYPE in ('AUTO','HOME','RENT') then APPR; return;
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then 'APPR'
            -- SAS: if FRAUD_RISK = 'LOW' and CLAIMED_AMOUNT <= SUM_INSURED * 0.25
            --      and CLAIMED_AMOUNT <= 50000 then APPR; return;
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then 'APPR'
            -- SAS: everything else → PEND; output MANUAL_REVIEW;
            else 'PEND'
        end as adjudication_result,

        -- Adjudication reason (source-faithful text)
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
            -- SAS: catx('; ', ifc(FRAUD_RISK='MEDIUM', ...), ifc(...), ...)
            else concat_ws('; ',
                case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end,
                case when claimed_amount > 50000 then 'Large claim' end,
                case
                    when claimed_amount > sum_insured * 0.25
                    then 'Exceeds 25% threshold'
                end
            )
        end as adjudication_reason,

        -- Approved amount
        case
            -- SAS: DENY → APPROVED_AMOUNT = 0
            when fraud_risk = 'HIGH'
                then 0
            -- SAS: APPR → APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE)
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT')
                then greatest(0, claimed_amount - deductible)
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000
                then greatest(0, claimed_amount - deductible)
            -- SAS: PEND → APPROVED_AMOUNT = . (missing)
            else null
        end as approved_amount

    from fraud_screened
)

select * from adjudicated
