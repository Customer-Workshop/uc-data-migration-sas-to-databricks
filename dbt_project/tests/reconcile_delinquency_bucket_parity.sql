/*
  Reconciliation: delinquency bucket assignment parity.

  Independently re-derives the SAS delinquency CASE from source data and
  compares (account_type, bucket, n_accounts) with the mart. Any row returned
  means the conversion diverged from the source bucket-assignment logic.

  dbt singular test: returns rows on divergence → fails the build.
*/
with source_buckets as (
    select
        a.account_type,
        case
            when coalesce(p.max_days_past_due_ever, 0) = 0
                then 'Current'
            when coalesce(p.max_days_past_due_ever, 0) between 1 and 29
                then '1-29'
            when coalesce(p.max_days_past_due_ever, 0) between 30 and 59
                then '30-59'
            when coalesce(p.max_days_past_due_ever, 0) between 60 and 89
                then '60-89'
            when coalesce(p.max_days_past_due_ever, 0) between 90 and 119
                then '90-119'
            when coalesce(p.max_days_past_due_ever, 0) between 120 and 179
                then '120-179'
            when coalesce(p.max_days_past_due_ever, 0) >= 180
                then '180+'
            else 'Unknown'
        end as delinq_bucket,
        count(*) as n
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
    group by 1, 2
),

mart_buckets as (
    select
        account_type,
        delinq_bucket,
        sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
    group by 1, 2
)

select
    coalesce(s.account_type, m.account_type) as account_type,
    coalesce(s.delinq_bucket, m.delinq_bucket) as delinq_bucket,
    s.n as expected_n,
    m.n as actual_n
from source_buckets s
full outer join mart_buckets m
    on s.account_type = m.account_type
    and s.delinq_bucket = m.delinq_bucket
where s.n is null or m.n is null or s.n <> m.n
