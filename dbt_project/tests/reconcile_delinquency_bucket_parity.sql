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
),

source_buckets as (
    select distinct
        case
            when p.max_days_past_due_ever = 0              then 'Current'
            when p.max_days_past_due_ever between 1 and 29     then '1-29'
            when p.max_days_past_due_ever between 30 and 59    then '30-59'
            when p.max_days_past_due_ever between 60 and 89    then '60-89'
            when p.max_days_past_due_ever between 90 and 119   then '90-119'
            when p.max_days_past_due_ever between 120 and 179  then '120-179'
            when p.max_days_past_due_ever >= 180           then '180+'
            else 'Unknown'
        end as bucket
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
)

/* Case (a): model bucket not in expected set */
select
    m.bucket as delinq_bucket,
    'UNEXPECTED_BUCKET' as issue
from model_buckets m
left join valid_buckets v on m.bucket = v.bucket
where v.bucket is null

union all

/* Case (b): expected bucket with source data absent from model */
select
    s.bucket as delinq_bucket,
    'MISSING_BUCKET' as issue
from source_buckets s
left join model_buckets m on s.bucket = m.bucket
where m.bucket is null
