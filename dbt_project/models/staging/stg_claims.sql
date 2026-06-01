/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1: Ingest and Validate)

  SAS Original:
    DATA WORK.CLAIMS_VALID / WORK.CLAIMS_INVALID;
      set RAW_INS.CLAIMS_FEED_YYYYMMDD;
      - hash lookup h_pol on RAW_INS.POLICIES(where=(STATUS='ACTIVE')) by POLICY_ID
        -> if rc ne 0           : "Policy not found or inactive"   -> CLAIMS_INVALID
      - if LOSS_DATE < EFFECTIVE_DATE or > EXPIRATION_DATE
                                : "Loss date outside policy period" -> CLAIMS_INVALID
      - if CLAIMED_AMOUNT > SUM_INSURED
                                : "Claimed amount exceeds sum insured" -> CLAIMS_INVALID
      - else                    : CLAIMS_VALID

  dbt Equivalent:
    - The daily feed RAW_INS.CLAIMS_FEED_YYYYMMDD maps to source('insurance_raw','claims').
    - The SAS hash object lookup against the ACTIVE policy master becomes a broadcast
      JOIN to the active subset of policies.
    - The DATA-step IF/THEN reject routing becomes a SQL CASE that reproduces the SAS
      validation order verbatim, then a WHERE that keeps only the validated (in-scope)
      claims (the CLAIMS_VALID branch). Raw claim columns are passed through unchanged;
      the policy attributes needed by downstream adjudication are carried forward.

  NOTE (source-faithful column mapping, not a logic change):
    The legacy POLICIES master exposed STATUS / EXPIRATION_DATE; the raw table here
    uses policy_status / expiry_date. These are renamed 1:1 below.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

-- SAS: declare hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))")
active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiry_date as expiration_date,
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
        p.expiration_date,
        p.sum_insured,
        p.deductible,

        -- SAS validation order is significant: the first failing check wins
        -- (each branch does `output WORK.CLAIMS_INVALID; return;`).
        case
            when p.policy_id is null
                then 'Policy not found or inactive: ' || c.policy_id
            when c.loss_date < p.effective_date or c.loss_date > p.expiration_date
                then 'Loss date outside policy period'
            when c.claimed_amount > p.sum_insured
                then 'Claimed amount exceeds sum insured'
            else null
        end as validation_error

    from claims c
    left join active_policies p
        on c.policy_id = p.policy_id
)

-- SAS CLAIMS_VALID branch: everything that passed every validation check.
select
    claim_id,
    policy_id,
    claimant_id,
    claim_type,
    claim_status,
    claimed_amount,
    loss_date,
    reported_date,
    policy_type,
    effective_date,
    expiration_date,
    sum_insured,
    deductible
from validated
where validation_error is null
