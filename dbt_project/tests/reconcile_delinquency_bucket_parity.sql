/*
  Reconciliation test: every delinquency bucket boundary mirrors the SAS
  DAYS_PAST_DUE CASE, including Unknown for null and negative values.
*/
with expected_accounts as (
    select
        '{{ var("prev_ym") }}' as report_month,
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
        report_month,
        account_type,
        region_code,
        delinq_bucket,
        count(*) as n_accounts
    from expected_accounts
    group by report_month, account_type, region_code, delinq_bucket
),

actual as (
    select
        report_month,
        account_type,
        region_code,
        delinq_bucket,
        n_accounts
    from {{ ref('mart_delinquency_aging') }}
)

select
    coalesce(e.report_month, a.report_month) as report_month,
    coalesce(e.account_type, a.account_type) as account_type,
    coalesce(e.region_code, a.region_code) as region_code,
    coalesce(e.delinq_bucket, a.delinq_bucket) as delinq_bucket,
    e.n_accounts as expected_accounts,
    a.n_accounts as model_accounts
from expected e
full outer join actual a
    on e.report_month = a.report_month
    and e.account_type = a.account_type
    and e.region_code = a.region_code
    and e.delinq_bucket = a.delinq_bucket
where e.n_accounts is null
   or a.n_accounts is null
   or e.n_accounts <> a.n_accounts
