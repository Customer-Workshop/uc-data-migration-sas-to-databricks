/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1: Ingest and Validate)

  SAS Original:
    DATA step with a hash object lookup to RAW_INS.POLICIES(where=STATUS='ACTIVE')
    routing rows to CLAIMS_VALID / CLAIMS_INVALID based on:
      - policy exists and is active
      - loss date within policy effective..expiration window
      - claimed amount does not exceed sum insured

  dbt Equivalent:
    The SAS hash-object lookup becomes a broadcast JOIN to the policies source.
    The IF/THEN output routing becomes a WHERE filter that keeps only valid claims.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select
        policy_id,
        policy_type,
        sum_insured,
        deductible,
        effective_date,
        expiry_date
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
),

validated as (
    select
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.claim_type,
        c.claim_status,
        c.claimed_amount,
        c.loss_date,
        c.reported_date,
        -- policy enrichment carried forward for adjudication (Step 2-4)
        p.policy_type,
        p.sum_insured,
        p.deductible
    from claims c
    inner join active_policies p
        on c.policy_id = p.policy_id
    -- SAS: loss date within policy period
    where c.loss_date between p.effective_date and p.expiry_date
    -- SAS: claimed amount must not exceed sum insured
      and c.claimed_amount <= p.sum_insured
)

select * from validated
