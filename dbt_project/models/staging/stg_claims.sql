/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step with hash object (declare hash h_pol) lookup to
    RAW_INS.POLICIES(where=STATUS='ACTIVE') for validation.
    Checks: policy exists/active, loss date within policy period,
    claimed amount <= sum insured. Invalid claims routed to
    WORK.CLAIMS_INVALID; valid claims to WORK.CLAIMS_VALID.

  dbt Equivalent:
    SQL LEFT JOIN replaces SAS hash object.
    CASE-based validation replaces DATA step IF/THEN routing.
    Only accepted (valid) records are emitted.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiration_date,
        sum_insured,
        deductible
    from {{ source('insurance_raw', 'policies') }}
    where status = 'ACTIVE'
),

validated as (
    select
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.claim_type,
        c.claim_status,
        c.loss_date,
        c.reported_date,
        c.claimed_amount,
        c.incurred_amount,
        c.paid_amount,
        c.reserved_amount,
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.sum_insured,
        p.deductible,
        case
            when p.policy_id is null
                then 'Policy not found or inactive'
            when c.loss_date < p.effective_date
                 or c.loss_date > p.expiration_date
                then 'Loss date outside policy period'
            when c.claimed_amount > p.sum_insured
                then 'Claimed amount exceeds sum insured'
            else null
        end as rejection_reason
    from claims c
    left join active_policies p
        on c.policy_id = p.policy_id
),

accepted as (
    select * from validated
    where rejection_reason is null
)

select * from accepted
