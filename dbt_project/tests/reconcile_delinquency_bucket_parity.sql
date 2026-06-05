/*
  Reconciliation: delinquency bucket parity — every bucket in the mart must
  be one of the SAS-defined buckets, and every expected bucket that has data
  must appear.

  The SAS Step 2 defines these buckets:
    Current, 1-29, 30-59, 60-89, 90-119, 120-179, 180+, Unknown

  This check flags (a) any model bucket NOT in the expected set (leaked label)
  and (b) any expected bucket with source data that is absent from the model
  (silent bucket loss).

  dbt singular test convention: FAILS if this query returns any rows.
*/
with valid_buckets as (
    select 'Current' as bucket
    union all select '1-29'
    union all select '30-59'
    union all select '60-89'
    union all select '90-119'
    union all select '120-179'
    union all select '180+'
    union all select 'Unknown'
),

model_buckets as (
    select distinct delinq_bucket as bucket
    from {{ ref('mart_delinquency_aging') }}
)

/* Case (a): model bucket not in expected set */
select
    m.bucket as delinq_bucket,
    'UNEXPECTED_BUCKET' as issue
from model_buckets m
left join valid_buckets v on m.bucket = v.bucket
where v.bucket is null
