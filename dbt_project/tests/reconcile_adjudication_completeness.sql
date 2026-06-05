/*
  Reconciliation test: adjudication completeness — all valid claims from
  stg_claims appear in int_claims_adjudication (no silent row loss).

  The SAS DATA step (claims_processing.sas, Step 3) processes every row from
  WORK.FRAUD_CHECK (which is built from WORK.CLAIMS_VALID) and outputs to
  either AUTO_ADJUDICATED or MANUAL_REVIEW — every input row goes somewhere.

  This test verifies 1:1 completeness between stg_claims and
  int_claims_adjudication: same count and no claim_id present in staging
  but missing from intermediate.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with staging_claims as (
    select count(*) as n from {{ ref('stg_claims') }}
),

adjudicated_claims as (
    select count(*) as n from {{ ref('int_claims_adjudication') }}
),

-- Check 1: row counts must match
count_mismatch as (
    select
        s.n as stg_count,
        a.n as adj_count,
        s.n - a.n as difference
    from staging_claims s
    cross join adjudicated_claims a
    where s.n <> a.n
),

-- Check 2: no orphan claim_ids (staging rows missing from adjudication)
orphan_claims as (
    select s.claim_id
    from {{ ref('stg_claims') }} s
    left join {{ ref('int_claims_adjudication') }} a
        on s.claim_id = a.claim_id
    where a.claim_id is null
)

select
    'count_mismatch' as check_type,
    cast(stg_count as string) as check_detail
from count_mismatch

union all

select
    'orphan_claim' as check_type,
    claim_id as check_detail
from orphan_claims
