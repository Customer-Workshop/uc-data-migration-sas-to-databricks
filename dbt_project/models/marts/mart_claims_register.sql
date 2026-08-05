/*
  mart_claims_register.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 4)

  SAS PROC APPEND incrementally extends STG_INS.CLAIMS_REGISTER. dbt
  materializes the full current table instead; this model deliberately
  preserves the output rows and columns without inventing persistence logic.
  SAS "&proc_date"d maps to current_date() for the dbt run date.
*/

{{
    config(
        materialized='table'
    )
}}

select
    claim_id,
    policy_id,
    claimant_id,
    claim_type,
    claim_status as source_claim_status,
    claimed_amount,
    loss_date,
    reported_date,
    policy_type,
    effective_date,
    expiry_date,
    sum_insured,
    deductible,
    fraud_score,
    model_version,
    fraud_risk,
    adjudication_result,
    adjudication_reason,
    approved_amount,
    routing_target,
    current_date() as processing_date,
    adjudication_result as claim_status,
    {{ format_claim_status('adjudication_result') }} as claim_status_desc
from {{ ref('int_claims_adjudication') }}
