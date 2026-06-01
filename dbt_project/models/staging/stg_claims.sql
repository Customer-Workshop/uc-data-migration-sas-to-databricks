/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step ingesting RAW_INS.CLAIMS_FEED, validating against
    RAW_INS.POLICIES via hash object (h_pol). Three validations:
      1. Policy exists and is ACTIVE
      2. Loss date within [EFFECTIVE_DATE, EXPIRATION_DATE]
      3. Claimed amount <= SUM_INSURED
    Invalid claims routed to WORK.CLAIMS_INVALID; valid to WORK.CLAIMS_VALID.

  dbt Equivalent:
    Broadcast JOIN to policies replaces the SAS hash object lookup.
    Invalid claims are filtered out (WHERE conditions) rather than
    routed to a separate output — dbt is set-based, not row-routing.

  Source-parity note:
    SAS filtered policies with STATUS='ACTIVE'; seed data uses
    policy_status='ACTIVE'. Column name adapted, logic preserved.
    SAS used EXPIRATION_DATE; seed data uses expiry_date.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
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
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
)

select * from validated
