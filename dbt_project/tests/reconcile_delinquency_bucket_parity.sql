/*
  Reconciliation control: delinquency bucket parity, per value.

  For every in-scope account, the bucket the mart assigned must match the SAS
  CASE from monthly_regulatory_reporting.sas Step 2 value-for-value:

    days_past_due = 0          -> 'Current'
    days_past_due 1-29         -> '1-29'
    days_past_due 30-59        -> '30-59'
    days_past_due 60-89        -> '60-89'
    days_past_due 90-119       -> '90-119'
    days_past_due 120-179      -> '120-179'
    days_past_due >= 180       -> '180+'
    else (NULL / negative)     -> 'Unknown'

  Expected buckets are recomputed per account from raw days_past_due and compared
  to the mart at (account_type, region_code, bucket) grain. Any mismatch leaves
  an unmatched row on one side of the full outer join and fails.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_per_account as (
    select
        account_type,
        region_code,
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            else 'Unknown'
        end as bucket
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

expected_summary as (
    select
        account_type,
        region_code,
        bucket,
        count(*) as n_expected
    from expected_per_account
    group by account_type, region_code, bucket
),

mart_summary as (
    select
        account_type,
        region_code,
        delinq_bucket as bucket,
        sum(n_accounts) as n_actual
    from {{ ref('mart_delinquency_aging') }}
    group by account_type, region_code, delinq_bucket
)

select
    coalesce(e.account_type, m.account_type) as account_type,
    coalesce(e.region_code, m.region_code) as region_code,
    coalesce(e.bucket, m.bucket) as bucket,
    e.n_expected,
    m.n_actual
from expected_summary e
full outer join mart_summary m
    on e.account_type = m.account_type
    and e.region_code = m.region_code
    and e.bucket = m.bucket
where e.account_type is null
   or m.account_type is null
   or coalesce(e.n_expected, -1) <> coalesce(m.n_actual, -1)
