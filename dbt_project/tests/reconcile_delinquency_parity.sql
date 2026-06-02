/*
  Reconciliation test: delinquency aging-bucket PARITY against the SAS source.

  monthly_regulatory_reporting.sas (Step 2) assigns each in-scope account to an
  aging bucket via a CASE on DAYS_PAST_DUE (0 -> Current; 1-29; 30-59; 60-89;
  90-119; 120-179; >=180 -> 180+; else -> Unknown). The mart must reproduce that
  bucket assignment value-for-value.

  This re-derives the expected bucket per account from int_account_metrics
  (+ payment_history.max_days_past_due_ever, the migrated days-past-due measure)
  using the exact SAS CASE, aggregates to (account_type, region_code, bucket, count),
  and full-outer-joins to the mart's own (account_type, region_code, delinq_bucket,
  n_accounts). Any mis-bucketed account surfaces as a returned row and fails the build.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with derived as (
    select
        a.account_type,
        a.region_code,
        case
            when p.max_days_past_due_ever = 0 then 'Current'
            when p.max_days_past_due_ever between 1 and 29 then '1-29'
            when p.max_days_past_due_ever between 30 and 59 then '30-59'
            when p.max_days_past_due_ever between 60 and 89 then '60-89'
            when p.max_days_past_due_ever between 90 and 119 then '90-119'
            when p.max_days_past_due_ever between 120 and 179 then '120-179'
            when p.max_days_past_due_ever >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

expected as (
    select
        account_type,
        region_code,
        delinq_bucket,
        count(*) as n
    from derived
    group by account_type, region_code, delinq_bucket
),

actual as (
    select
        account_type,
        region_code,
        delinq_bucket,
        sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
    group by account_type, region_code, delinq_bucket
)

select
    coalesce(e.account_type, a.account_type) as account_type,
    coalesce(e.region_code, a.region_code) as region_code,
    coalesce(e.delinq_bucket, a.delinq_bucket) as delinq_bucket,
    e.n as expected_accounts,
    a.n as actual_accounts
from expected e
full outer join actual a
    on e.account_type = a.account_type
    and e.region_code = a.region_code
    and e.delinq_bucket = a.delinq_bucket
where coalesce(e.n, 0) <> coalesce(a.n, 0)
