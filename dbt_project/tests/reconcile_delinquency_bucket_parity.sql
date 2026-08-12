/*
  Reconciliation: delinquency bucket parity against the SAS bands.

  Recomputes the aging bucket per account directly from the SAS CASE
  (including the 'Unknown' catch-all for a missing DAYS_PAST_DUE) and
  compares per-bucket account counts to the mart. Catches an off-by-one
  band boundary or a dropped/renamed bucket, not just a total that
  happens to tie out.

  dbt singular test convention: FAILS if this query returns rows.
*/
with expected as (
    select
        case
            when l.days_past_due = 0 then 'Current'
            when l.days_past_due between 1 and 29 then '1-29'
            when l.days_past_due between 30 and 59 then '30-59'
            when l.days_past_due between 60 and 89 then '60-89'
            when l.days_past_due between 90 and 119 then '90-119'
            when l.days_past_due between 120 and 179 then '120-179'
            when l.days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket,
        count(*) as n
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
    group by 1
),

actual as (
    select
        delinq_bucket,
        sum(n_accounts) as n
    from {{ ref('mart_delinquency_aging') }}
    group by 1
)

select
    coalesce(e.delinq_bucket, a.delinq_bucket) as delinq_bucket,
    e.n as expected_accounts,
    a.n as mart_accounts
from expected e
full outer join actual a
    on e.delinq_bucket = a.delinq_bucket
where e.n is null or a.n is null or e.n <> a.n
