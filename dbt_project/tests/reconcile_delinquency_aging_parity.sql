/*
  Reconciliation test: REPORTS.DELINQUENCY_AGING completeness and bucket parity.
  Program: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  Two things are checked against the raw source:
    * completeness — the report's N_ACCOUNTS add up to every in-scope lending
      account at the month-end snapshot (SAS: ACCOUNT_TYPE in
      MTG/AUTO/PERS/CC/LOC/HELC, LEFT join to LOAN_DETAILS so nothing is lost);
    * bucket parity — each DAYS_PAST_DUE bucket boundary is re-derived from the
      SAS ladder written as a literal bucket table and compared value-for-value
      (counts and balances) with the mart, per account type and region.

  Source-faithful: an account with no LOAN_DETAILS row has DAYS_PAST_DUE
  missing, which matches none of the SAS comparisons and lands in 'Unknown'.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_population as (
    select
        a.account_type,
        a.region_code,
        a.current_balance,
        l.days_past_due,
        l.past_due_amount
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.snapshot_date = {{ sas_month_end() }}
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

sas_buckets as (
    select *
    from (
        values
        ('Current', 0, 0),
        ('1-29', 1, 29),
        ('30-59', 30, 59),
        ('60-89', 60, 89),
        ('90-119', 90, 119),
        ('120-179', 120, 179),
        ('180+', 180, 2147483647)
    ) as b (bucket, low, high)
),

expected as (
    select
        s.account_type,
        s.region_code,
        coalesce(b.bucket, 'Unknown') as delinq_bucket,
        count(*) as n_accounts,
        round(sum(s.current_balance), 2) as total_balance,
        round(sum(s.past_due_amount), 2) as total_past_due
    from source_population s
    left join sas_buckets b
        on s.days_past_due between b.low and b.high
    group by s.account_type, s.region_code, coalesce(b.bucket, 'Unknown')
),

reported as (
    select
        account_type,
        region_code,
        delinq_bucket,
        n_accounts,
        round(total_balance, 2) as total_balance,
        round(total_past_due, 2) as total_past_due
    from {{ ref('mart_delinquency_aging') }}
)

select
    coalesce(e.account_type, r.account_type) as account_type,
    coalesce(e.region_code, r.region_code) as region_code,
    coalesce(e.delinq_bucket, r.delinq_bucket) as delinq_bucket,
    e.n_accounts as sas_n_accounts,
    r.n_accounts as mart_n_accounts,
    e.total_balance as sas_total_balance,
    r.total_balance as mart_total_balance,
    e.total_past_due as sas_total_past_due,
    r.total_past_due as mart_total_past_due
from expected e
full outer join reported r
    on e.account_type = r.account_type
    and e.region_code = r.region_code
    and e.delinq_bucket = r.delinq_bucket
where e.n_accounts is null
   or r.n_accounts is null
   or e.n_accounts <> r.n_accounts
   or abs(coalesce(e.total_balance, 0) - coalesce(r.total_balance, 0)) > 0.01
   or abs(coalesce(e.total_past_due, 0) - coalesce(r.total_past_due, 0)) > 0.01
