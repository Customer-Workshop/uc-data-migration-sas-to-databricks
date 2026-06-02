/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step with hash object (h_pol) lookup to RAW_INS.POLICIES
    (where=(STATUS='ACTIVE')). Validates policy existence, loss date
    within policy period, and claimed amount vs sum insured. Invalid
    claims routed to WORK.CLAIMS_INVALID (dropped); valid claims to
    WORK.CLAIMS_VALID with policy enrichment columns.

  dbt Equivalent:
    INNER JOIN replaces hash object lookup (broadcast-eligible);
    WHERE clause replicates the three validation gates, emitting
    only the CLAIMS_VALID population.
*/

with claims_feed as (
    -- SAS: set RAW_INS.&feed_ds
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    -- SAS: declare hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))")
    -- SAS: h_pol.definekey('POLICY_ID')
    -- SAS: h_pol.definedata('POLICY_TYPE','EFFECTIVE_DATE','EXPIRATION_DATE',
    --                       'SUM_INSURED','DEDUCTIBLE')
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
        c.claimed_amount,
        c.loss_date,
        c.reported_date,
        -- SAS: columns brought in by h_pol.definedata()
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.sum_insured,
        p.deductible
    from claims_feed c
    -- SAS: rc = h_pol.find(); if rc ne 0 → output CLAIMS_INVALID; return;
    inner join active_policies p
        on c.policy_id = p.policy_id
    -- SAS: if LOSS_DATE < EFFECTIVE_DATE or LOSS_DATE > EXPIRATION_DATE → CLAIMS_INVALID
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiration_date
      -- SAS: if CLAIMED_AMOUNT > SUM_INSURED → CLAIMS_INVALID
      and c.claimed_amount <= p.sum_insured
)

select * from validated
