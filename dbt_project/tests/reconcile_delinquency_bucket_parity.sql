/*
  Reconciliation control: delinquency bucket parity with the SAS source.

  Every bucket boundary in monthly_regulatory_reporting.sas Step 2 is pinned
  here against counts recomputed straight from raw DAYS_PAST_DUE:

      = 0 -> Current      60-89   -> 60-89      >= 180 -> 180+
      1-29                90-119                missing/no loan row -> Unknown
      30-59               120-179

  An off-by-one boundary (`between 30 and 59` written as `> 30`), a lost
  'Unknown' population, or a mis-typed label shifts accounts between buckets and
  fails here even though the row count and the balance total still tie out.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with in_scope_loans as (
    select l.days_past_due
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

expected as (
    select
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket,
        count(*) as n_accounts
    from in_scope_loans
    group by 1
),

actual as (
    select
        delinq_bucket,
        sum(n_accounts) as n_accounts
    from {{ ref('mart_delinquency_aging') }}
    group by 1
)

select
    coalesce(e.delinq_bucket, a.delinq_bucket) as delinq_bucket,
    e.n_accounts as expected_accounts,
    a.n_accounts as actual_accounts
from expected e
full outer join actual a
    on e.delinq_bucket = a.delinq_bucket
where coalesce(e.n_accounts, -1) <> coalesce(a.n_accounts, -1)
