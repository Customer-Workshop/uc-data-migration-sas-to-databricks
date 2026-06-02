/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step with hash object lookup to RAW_INS.POLICIES(where=(STATUS='ACTIVE')),
    validating policy existence, loss date within policy period, and claimed amount
    within sum insured. Invalid claims route to WORK.CLAIMS_INVALID (dropped here);
    valid claims to WORK.CLAIMS_VALID.

  dbt Equivalent:
    INNER JOIN replaces SAS hash-object lookup (h_pol.find()).
    WHERE clauses replicate the three validation filters.
    Only valid claims are emitted (CLAIMS_INVALID records are excluded).

  Source-faithful notes:
    - SAS column STATUS maps to policy_status in Databricks raw schema.
    - SAS EXPIRATION_DATE maps to expiry_date in Databricks raw schema.
    - SAS reads from daily feed RAW_INS.CLAIMS_FEED_YYYYMMDD; Databricks
      reads from the consolidated claims table in Unity Catalog.
*/

with claims_feed as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    -- SAS: declare hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))");
    select * from {{ source('insurance_raw', 'policies') }}
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
    -- SAS: if LOSS_DATE < EFFECTIVE_DATE or LOSS_DATE > EXPIRATION_DATE then INVALID
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
    -- SAS: if CLAIMED_AMOUNT > SUM_INSURED then INVALID
      and c.claimed_amount <= p.sum_insured
)

select * from validated
