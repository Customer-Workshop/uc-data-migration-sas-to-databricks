/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step with hash object lookup to RAW_INS.POLICIES for validation:
    - Policy must exist and be ACTIVE
    - Loss date must fall within policy period
    - Claimed amount must not exceed sum insured
    Invalid claims routed to WORK.CLAIMS_INVALID

  dbt Equivalent:
    Broadcast JOIN replaces SAS hash object lookup (h_pol)
    SQL CASE/WHERE replaces DATA step IF/THEN validation routing
    Rejected records excluded via WHERE (equivalent to OUTPUT to CLAIMS_VALID only)
*/

with claims_feed as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select /*+ BROADCAST(p) */
        policy_id,
        policy_type,
        effective_date,
        expiration_date,
        sum_insured,
        deductible
    from {{ source('insurance_raw', 'policies') }}
    where status = 'ACTIVE'
),

-- SAS: hash object lookup (h_pol.find()) + validation IF/THEN blocks
validated as (
    select
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.claim_type,
        c.claim_status,
        c.claimed_amount,
        c.incurred_amount,
        c.paid_amount,
        c.reserved_amount,
        c.loss_date,
        c.reported_date,
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.sum_insured,
        p.deductible,
        case
            when p.policy_id is null
                then 'Policy not found or inactive'
            when c.loss_date < p.effective_date or c.loss_date > p.expiration_date
                then 'Loss date outside policy period'
            when c.claimed_amount > p.sum_insured
                then 'Claimed amount exceeds sum insured'
            else null
        end as validation_error
    from claims_feed c
    left join active_policies p
        on c.policy_id = p.policy_id
),

-- Equivalent of SAS "output WORK.CLAIMS_VALID" path
accepted as (
    select * from validated
    where validation_error is null
)

select * from accepted
