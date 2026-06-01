/*
  Reconciliation: delinquency bucket parity.

  Independently compute the delinquency bucket from source data using the
  SAS CASE mapping and compare to the mart. A full outer join on
  (account_type, region_code, bucket) catches both incorrect buckets and
  mismatched counts / balances.

  SAS mapping (monthly_regulatory_reporting.sas, Step 2):
    days_past_due = 0            -> 'Current'
    days_past_due 1-29           -> '1-29'
    days_past_due 30-59          -> '30-59'
    days_past_due 60-89          -> '60-89'
    days_past_due 90-119         -> '90-119'
    days_past_due 120-179        -> '120-179'
    days_past_due >= 180         -> '180+'
    else (NULL)                  -> 'Unknown'
*/
with source_buckets as (
    select
        account_type,
        region_code,
        case
            when days_past_due = 0                  then 'Current'
            when days_past_due between 1 and 29     then '1-29'
            when days_past_due between 30 and 59    then '30-59'
            when days_past_due between 60 and 89    then '60-89'
            when days_past_due between 90 and 119   then '90-119'
            when days_past_due between 120 and 179  then '120-179'
            when days_past_due >= 180               then '180+'
            else 'Unknown'
        end as expected_bucket,
        count(*) as expected_n,
        sum(current_balance) as expected_balance
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
    group by 1, 2, 3
),

mart_buckets as (
    select
        account_type,
        region_code,
        delinq_bucket,
        n_accounts,
        total_balance
    from {{ ref('mart_delinquency_aging') }}
)

select
    coalesce(s.account_type, m.account_type) as account_type,
    coalesce(s.region_code, m.region_code) as region_code,
    s.expected_bucket,
    m.delinq_bucket as actual_bucket,
    s.expected_n,
    m.n_accounts as actual_n,
    s.expected_balance,
    m.total_balance as actual_balance
from source_buckets s
full outer join mart_buckets m
    on s.account_type = m.account_type
    and s.region_code = m.region_code
    and s.expected_bucket = m.delinq_bucket
where s.expected_bucket is null
    or m.delinq_bucket is null
    or s.expected_n <> m.n_accounts
    or abs(coalesce(s.expected_balance, 0) - coalesce(m.total_balance, 0)) > 0.01
