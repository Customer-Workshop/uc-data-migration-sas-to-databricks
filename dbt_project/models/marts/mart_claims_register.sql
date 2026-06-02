/*
  mart_claims_register.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 4)

  SAS Original:
    DATA WORK.CLAIMS_COMBINED combining AUTO_ADJUDICATED + MANUAL_REVIEW,
    setting PROCESSING_DATE and CLAIM_STATUS = ADJUDICATION_RESULT,
    with format CLAIM_STATUS $CLMSTAT.
    PROC APPEND to STG_INS.CLAIMS_REGISTER.

  dbt Equivalent:
    SELECT from int_claims_adjudication with final presentation columns.
    Materialized as table (replaces PROC APPEND to permanent dataset).
    $CLMSTAT format values: APPR='Approved', DENY='Denied', PEND='Pending Approval'.
*/

select
    claim_id,
    policy_id,
    claimant_id,
    claim_type,
    policy_type,
    claimed_amount,
    approved_amount,
    loss_date,
    reported_date,
    fraud_score,
    fraud_risk,
    adjudication_result,
    adjudication_reason,
    -- SAS: CLAIM_STATUS = ADJUDICATION_RESULT; format CLAIM_STATUS $CLMSTAT.;
    adjudication_result as processing_status,
    -- SAS: $CLMSTAT format applied for display
    case adjudication_result
        when 'APPR' then 'Approved'
        when 'DENY' then 'Denied'
        when 'PEND' then 'Pending Approval'
        else 'Unknown'
    end as processing_status_desc,
    current_date() as processing_date

from {{ ref('int_claims_adjudication') }}
