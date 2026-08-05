/*
  mart_delinquency_aging.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 2)

  The source population is int_account_metrics restricted to the SAS
  account types MTG, AUTO, PERS, CC, LOC, and HELC. The migrated raw estate
  has no DAYS_PAST_DUE column on loan_details, so the payment_history
  max_days_past_due_ever field is the documented substitution.

  PAST_DUE_AMOUNT has no counterpart anywhere in raw. It is emitted as zero
  rather than fabricated; consequently total_past_due is not yet a
  meaningful reconciliation control. This data gap is carried into the PR.

  SAS-to-dbt migration gaps:
    - REPORTS.LLP_COVERAGE (SAS Step 3) is not converted faithfully because
      ALLOWANCE_AMT and DAYS_PAST_DUE are unavailable in the raw estate.
    - PROC EXPORT to XLSX (SAS Step 4) is out of scope; see
      docs/SAS_TO_DBT_MIGRATION_MAP.md.
*/

with account_delinquency as (
    select
        '{{ var("prev_ym") }}' as report_month,
        a.account_type,
        a.region_code,
        a.current_balance,
        p.max_days_past_due_ever as days_past_due
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

bucketed as (
    select
        report_month,
        account_type,
        region_code,
        current_balance,
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
    from account_delinquency
)

select
    report_month,
    account_type,
    region_code,
    delinq_bucket,
    count(*) as n_accounts,
    sum(current_balance) as total_balance,
    sum(cast(0 as decimal(18, 2))) as total_past_due
from bucketed
group by report_month, account_type, region_code, delinq_bucket
order by
    account_type,
    region_code,
    case
        when delinq_bucket = 'Current' then 0
        when delinq_bucket = '1-29' then 1
        when delinq_bucket = '30-59' then 2
        when delinq_bucket = '60-89' then 3
        when delinq_bucket = '90-119' then 4
        when delinq_bucket = '120-179' then 5
        when delinq_bucket = '180+' then 6
        else 7
    end
