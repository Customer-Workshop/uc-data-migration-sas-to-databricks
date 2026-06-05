/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step with hash-object lookup on RAW_INS.POLICIES (STATUS='ACTIVE')
    to validate claims. Three validation rules gate output:
      1. Policy must exist and be active (hash find rc=0)
      2. Loss date within policy period (EFFECTIVE_DATE..EXPIRATION_DATE)
      3. Claimed amount <= sum_insured
    Invalid claims are routed to WORK.CLAIMS_INVALID; only valid claims
    pass through (RETURN exits iteration on first failing rule).

  dbt Equivalent:
    Broadcast JOIN to policies (WHERE status='ACTIVE') replaces the SAS
    hash-object lookup. The three validation rules are applied via WHERE,
    reproducing the SAS DATA step filtering exactly.
*/

with claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    -- SAS: declare hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))");
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
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.sum_insured,
        p.deductible
    from claims c
    inner join active_policies p
        on c.policy_id = p.policy_id
    -- SAS validation rule 1: policy must exist and be active (enforced by INNER JOIN)
    -- SAS validation rule 2: loss_date within policy period
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiration_date
    -- SAS validation rule 3: claimed_amount <= sum_insured
      and c.claimed_amount <= p.sum_insured
)

select * from validated
