/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  Source mappings and intentional divergences:
    - RAW_INS.CLAIMS_FEED_YYYYMMDD maps to insurance_raw.claims because Unity
      Catalog has no per-day feed table; the claims register stands in for it.
    - RAW_INS.POLICIES.STATUS maps to policy_status, and SAS
      EXPIRATION_DATE maps to expiry_date.
    - SAS hash lookup of active policies is an inner join here. Invalid claims
      are silently discarded because SAS never persists WORK.CLAIMS_INVALID.
    - SAS drops VALIDATION_ERROR and rc from every output, including the
      invalid output, so no validation reason is carried forward.
    - SAS numeric/date comparisons treat missing values as numeric missing.
      A missing CLAIMED_AMOUNT therefore passes the greater-than check.
*/

with claims as (
    select
        claim_id,
        policy_id,
        claimant_id,
        claim_type,
        claim_status,
        claimed_amount,
        loss_date,
        reported_date
    from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiry_date,
        sum_insured,
        deductible
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
        p.policy_type,
        p.effective_date,
        p.expiry_date,
        p.sum_insured,
        p.deductible
    from claims c
    inner join active_policies p
        on c.policy_id = p.policy_id
    where (
        c.loss_date is not null
        and c.loss_date >= p.effective_date
        and c.loss_date <= p.expiry_date
    )
      and (
          c.claimed_amount is null
          or c.claimed_amount <= p.sum_insured
      )
)

select * from validated
