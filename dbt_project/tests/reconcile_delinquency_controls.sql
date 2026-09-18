/*
  monthly_regulatory_reporting.sas Step 2 (DELINQUENCY_AGING)
    - completeness: sum(N_ACCOUNTS) = lending accounts in the month-end snapshot
    - control total: sum(TOTAL_BALANCE) = their CURRENT_BALANCE total
    - mapping parity: only the eight SAS bucket labels appear, each with its
      severity rank; only lending products appear
    - every bucket that has accounts in the base data appears in the report
*/
with base as (
    select a.account_id, a.account_type, a.current_balance, l.days_past_due
    from {{ ref('int_account_metrics') }} a
    left join {{ ref('stg_loan_details') }} l on a.account_id = l.account_id
    where a.snapshot_date = {{ sas_month_end() }}
      and a.account_type in {{ sas_lending_products() }}
),

report as (
    select * from {{ ref('mart_delinquency_aging') }}
),

totals as (
    select 'sum(N_ACCOUNTS) = lending accounts' as control, cast(b.n as string) as expected, cast(sum(r.n_accounts) as string) as actual
    from report r cross join (select count(*) as n from base) b group by b.n
    having b.n <> sum(r.n_accounts)
    union all
    select 'sum(TOTAL_BALANCE) = lending balance', cast(round(b.bal, 2) as string), cast(round(sum(r.total_balance), 2) as string)
    from report r cross join (select sum(current_balance) as bal from base) b group by b.bal
    having not {{ approx_equal('b.bal', 'sum(r.total_balance)') }}
),

mapping as (
    select
        concat('bucket/order ', delinq_bucket) as control,
        case delinq_bucket
            when 'Current' then '0' when '1-29' then '1' when '30-59' then '2' when '60-89' then '3'
            when '90-119' then '4' when '120-179' then '5' when '180+' then '6' when 'Unknown' then '7'
            else 'unknown label' end as expected,
        cast(bucket_sort_order as string) as actual
    from report
    where delinq_bucket not in ('Current', '1-29', '30-59', '60-89', '90-119', '120-179', '180+', 'Unknown')
       or bucket_sort_order <> case delinq_bucket
            when 'Current' then 0 when '1-29' then 1 when '30-59' then 2 when '60-89' then 3
            when '90-119' then 4 when '120-179' then 5 when '180+' then 6 else 7 end
       or account_type not in {{ sas_lending_products() }}
),

expected_buckets as (
    select distinct
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            else 'Unknown'
        end as delinq_bucket
    from base
),

missing_buckets as (
    select concat('bucket missing from report ', e.delinq_bucket) as control, e.delinq_bucket as expected, cast(null as string) as actual
    from expected_buckets e
    left join (select distinct delinq_bucket from report) r on e.delinq_bucket = r.delinq_bucket
    where r.delinq_bucket is null
)

select * from totals
union all
select * from mapping
union all
select * from missing_buckets
