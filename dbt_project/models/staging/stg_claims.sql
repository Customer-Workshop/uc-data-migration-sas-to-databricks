/*
  stg_claims.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 1)

  SAS Original:
    DATA step validating incoming claims feed (RAW_INS.CLAIMS_FEED_YYYYMMDD)
    with hash object lookup against RAW_INS.POLICIES for policy enrichment
    and validation (active policy, loss date within period, amount within
    sum insured).

  dbt Equivalent:
    Staging model reading from Databricks external table (Unity Catalog).
    SAS hash object lookup replaced by broadcast JOIN to policies.
    DATA step IF/THEN validation replaced by SQL CASE + WHERE filter.
*/

with claims_feed as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select /*+ BROADCAST(p) */
        policy_id,
        policy_type,
        effective_date,
        expiration_date,
        sum_insured,
        deductible
    from {{ source('insurance_raw', 'policies') }}
    where status = 'ACTIVE'
),

-- SAS: hash object lookup (h_pol.find()) + validation checks
validated as (
    select
        c.*,
        p.policy_type,
        p.effective_date   as policy_effective_date,
        p.expiration_date  as policy_expiration_date,
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
        end as validation_error
    from claims_feed c
    left join active_policies p
        on c.policy_id = p.policy_id
),

accepted as (
    select * from validated
    where validation_error is null
)

select * from accepted
