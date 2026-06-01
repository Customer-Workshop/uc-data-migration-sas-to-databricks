/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA WORK.CLAIMS_VALID / WORK.CLAIMS_INVALID;
      - Hash object lookup (h_pol) against RAW_INS.POLICIES(where=(STATUS='ACTIVE'))
      - Validates: policy exists & active, loss_date within policy period,
        claimed_amount <= sum_insured
      - Invalid claims are routed to CLAIMS_INVALID (dropped here; only valid claims pass)

  dbt Equivalent:
    Broadcast JOIN to policies replaces hash object lookup.
    WHERE filters replicate the three validation gates.
    Invalid-reason logic is preserved as comments for traceability but only
    valid rows are emitted (matching WORK.CLAIMS_VALID output).

  Column-name mapping (SAS -> Databricks):
    STATUS        -> policy_status
    EXPIRATION_DATE -> expiry_date

  Quirks reproduced from SAS source:
    - The SAS hash only loads STATUS='ACTIVE' policies. Claims against LAPSED
      or CANCELLED policies are silently treated as "policy not found" — same
      behaviour reproduced here via the WHERE on policy_status = 'ACTIVE'.
*/

with claims_feed as (
    -- SAS: RAW_INS.CLAIMS_FEED_YYYYMMDD
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    -- SAS: declare hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))")
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
    from claims_feed c
    inner join active_policies p
        on c.policy_id = p.policy_id
    -- Validation gate 1: policy exists and is active (enforced by INNER JOIN)
    -- Validation gate 2: loss_date within policy period
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
    -- Validation gate 3: claimed_amount <= sum_insured
      and c.claimed_amount <= p.sum_insured
)

select * from validated
