/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1: Ingest and Validate)

  SAS Original:
    data WORK.CLAIMS_VALID WORK.CLAIMS_INVALID;
      set RAW_INS.&feed_ds;                                  -- dated daily claims feed
      declare hash h_pol(dataset:"RAW_INS.POLICIES(where=(STATUS='ACTIVE'))");
      rc = h_pol.find();                                     -- policy lookup
      if rc ne 0                          -> CLAIMS_INVALID  (policy not found/inactive)
      if LOSS_DATE outside policy period  -> CLAIMS_INVALID
      if CLAIMED_AMOUNT > SUM_INSURED     -> CLAIMS_INVALID
      else                                -> CLAIMS_VALID
      drop VALIDATION_ERROR rc;

  dbt Equivalent:
    Staging model. The SAS hash-object lookup against active policies becomes an
    INNER-equivalent (LEFT JOIN + filter) broadcast join; the DATA-step IF/THEN
    routing becomes a SQL CASE validation_error + WHERE that emits only the valid
    rows (mirrors stg_daily_transactions's reject-routing pattern).

  Source-faithfulness notes:
    - SAS read a dated daily feed RAW_INS.CLAIMS_FEED_YYYYMMDD; the migrated raw
      source is insurance_raw.claims (the same claim records).
    - SAS policy columns EXPIRATION_DATE / STATUS map to raw policy columns
      expiry_date / policy_status.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

-- SAS: declare hash h_pol(dataset:"RAW_INS.POLICIES(where=(STATUS='ACTIVE'))")
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

-- SAS: rc = h_pol.find(); validation IF/THEN routing (CLAIMS_INVALID vs CLAIMS_VALID)
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
        p.deductible,
        case
            -- SAS: if rc ne 0 then 'Policy not found or inactive'
            when p.policy_id is null
                then 'Policy not found or inactive: ' || c.policy_id
            -- SAS: if LOSS_DATE < EFFECTIVE_DATE or LOSS_DATE > EXPIRATION_DATE
            when c.loss_date < p.effective_date or c.loss_date > p.expiry_date
                then 'Loss date outside policy period'
            -- SAS: if CLAIMED_AMOUNT > SUM_INSURED
            when c.claimed_amount > p.sum_insured
                then 'Claimed amount exceeds sum insured'
            else null
        end as validation_error
    from claims c
    left join active_policies p
        on c.policy_id = p.policy_id
),

-- SAS: output WORK.CLAIMS_VALID (only records with no validation error)
accepted as (
    select * from validated
    where validation_error is null
)

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
    expiry_date,
    sum_insured,
    deductible
from accepted
