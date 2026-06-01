/*
  int_claims_adjudication.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2-4)

  SAS Original:
    Step 2 (PROC SQL): left join WORK.CLAIMS_VALID to TERA_DW.FRAUD_INDICATORS
      on (POLICY_ID, CLAIMANT_ID); FRAUD_RISK = case fraud_score
        >= 80 -> HIGH ; >= 50 -> MEDIUM ; else LOW.
    Step 3 (DATA step, ordered IF/THEN with `return`): route each claim to the
      first matching rule -> WORK.AUTO_ADJUDICATED or WORK.MANUAL_REVIEW.
    Step 4 (DATA step): CLAIM_STATUS = ADJUDICATION_RESULT, formatted $CLMSTAT.

  dbt Equivalent:
    SAS hash/PROC SQL join -> LEFT JOIN; the ordered DATA-step IF/THEN/return
    chain -> a single ordered CASE that yields the matched rule number
    (adjudication_branch), from which result / reason / approved_amount /
    routing_target are derived value-for-value. SAS $CLMSTAT -> format_claim_status macro.

  ----------------------------------------------------------------------------
  SOURCE-FAITHFUL QUIRKS reproduced here (NOT defects to "fix" -- the
  reconciliation parity controls in tests/reconcile_claims_*.sql guard them):

  1. DENY is routed to MANUAL_REVIEW, not AUTO_ADJUDICATED. In the SAS Step 3
     the high-fraud branch sets ADJUDICATION_RESULT='DENY' but does
     `output WORK.MANUAL_REVIEW`. So an auto-decided DENY still lands in the
     manual review queue (with APPROVED_AMOUNT=0). The "obvious" conversion --
     sending DENY to the auto-adjudicated set because it is an automated
     decision -- DIVERGES from the source. routing_target encodes the SAS truth.

  2. A missing fraud score (claim with no row in the fraud feed) is treated as
     LOW. SAS does a LEFT JOIN and a missing numeric compares as < any value,
     so `fraud_score >= 50` is false and the claim falls to the LOW branch.
     The CASE below reproduces this because NULL >= 80/50 is not TRUE.

  3. The first auto-approve branch keeps the SAS policy-type list verbatim,
     including 'RENT', even though the current policy master carries no RENT
     rows. Reproduced value-for-value rather than trimmed.
  ----------------------------------------------------------------------------
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

fraud as (
    select
        policy_id,
        claimant_id,
        fraud_score,
        indicator_flags
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

-- SAS Step 2: fraud screening (LEFT JOIN keeps every valid claim; a claim with
-- no fraud row keeps a NULL score -> LOW below).
screened as (
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

-- SAS Step 3: ordered IF/THEN/return chain -> first matching rule wins.
branched as (
    select
        *,
        case
            -- Rule 1: auto-deny high fraud risk (SIU referral).
            when fraud_risk = 'HIGH' then 1
            -- Rule 2: auto-approve low risk, small claim on AUTO/HOME/RENT.
            when fraud_risk = 'LOW'
                 and claimed_amount <= 5000
                 and policy_type in ('AUTO', 'HOME', 'RENT') then 2
            -- Rule 3: auto-approve low risk within 25% of sum insured and <= 50k.
            when fraud_risk = 'LOW'
                 and claimed_amount <= sum_insured * 0.25
                 and claimed_amount <= 50000 then 3
            -- Rule 4: everything else -> manual review (PEND).
            else 4
        end as adjudication_branch
    from screened
),

adjudicated as (
    select
        claim_id,
        policy_id,
        claimant_id,
        claim_type,
        claimed_amount,
        loss_date,
        reported_date,
        policy_type,
        sum_insured,
        deductible,
        fraud_score,
        indicator_flags,
        fraud_risk,
        adjudication_branch,

        -- ADJUDICATION_RESULT
        case adjudication_branch
            when 1 then 'DENY'
            when 2 then 'APPR'
            when 3 then 'APPR'
            else 'PEND'
        end as adjudication_result,

        -- ADJUDICATION_REASON (value-for-value with the SAS strings; the PEND
        -- reason reproduces the SAS catx('; ', ifc(...), ifc(...), ifc(...))
        -- which drops empty parts -- concat_ws skips NULLs identically).
        case adjudication_branch
            when 1 then 'High fraud risk - SIU referral'
            when 2 then 'Auto-approved: low risk, small claim'
            when 3 then 'Auto-approved: within 25% of sum insured'
            else concat_ws(
                '; ',
                case when fraud_risk = 'MEDIUM' then 'Medium fraud risk' end,
                case when claimed_amount > 50000 then 'Large claim' end,
                case when claimed_amount > sum_insured * 0.25 then 'Exceeds 25% threshold' end
            )
        end as adjudication_reason,

        -- APPROVED_AMOUNT: DENY -> 0 ; APPR -> max(0, claimed - deductible) ;
        -- PEND -> missing (NULL).
        case adjudication_branch
            when 1 then 0
            when 2 then greatest(0, claimed_amount - deductible)
            when 3 then greatest(0, claimed_amount - deductible)
            else cast(null as double)
        end as approved_amount,

        -- SAS Step 3 routing target. Rule 1 (DENY) goes to MANUAL_REVIEW -- see
        -- QUIRK 1 in the header. Rules 2/3 (APPR) go to AUTO_ADJUDICATED.
        case adjudication_branch
            when 2 then 'AUTO_ADJUDICATED'
            when 3 then 'AUTO_ADJUDICATED'
            else 'MANUAL_REVIEW'
        end as routing_target,

        -- Step 2 SIU separation: FRAUD_ALERTS = where FRAUD_RISK='HIGH'.
        fraud_risk = 'HIGH' as fraud_alert_flag,
        case
            when fraud_risk = 'HIGH'
                then concat_ws(
                    '; ',
                    'Fraud score: ' || cast(cast(round(fraud_score, 0) as int) as string),
                    nullif(indicator_flags, '')
                )
        end as alert_reason

    from branched
)

select
    *,
    -- SAS Step 4: CLAIM_STATUS = ADJUDICATION_RESULT (formatted $CLMSTAT).
    adjudication_result as claim_status,
    {{ format_claim_status('adjudication_result') }} as claim_status_desc,
    current_date() as processing_date
from adjudicated
