/*
  mart_llp_coverage.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 3)
  Output contract: REPORTS.LLP_COVERAGE (replaced each run)

  SAS Original:
    Month-end snapshot INNER JOIN ORA_DW.LOAN_DETAILS, lending products only,
    group by REPORT_MONTH, ACCOUNT_TYPE:
      N_LOANS          = count(*)
      GROSS_LOANS      = sum(CURRENT_BALANCE)
      TOTAL_ALLOWANCE  = sum(ALLOWANCE_AMT)
      COVERAGE_PCT     = TOTAL_ALLOWANCE / GROSS_LOANS * 100 when GROSS_LOANS > 0 else 0
      NPL_BALANCE      = sum(CURRENT_BALANCE where DAYS_PAST_DUE >= 90 else 0)
      NPL_COVERAGE_PCT = TOTAL_ALLOWANCE / NPL_BALANCE * 100 when NPL_BALANCE > 0 else 0
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
      and account_type in {{ sas_lending_products() }}
),

loans as (
    select * from {{ ref('stg_loan_details') }}
),

joined as (
    select
        a.account_type,
        a.current_balance,
        l.allowance_amt,
        l.days_past_due
    from accounts a
    inner join loans l
        on a.account_id = l.account_id
),

aggregated as (
    select
        account_type,
        count(*) as n_loans,
        sum(current_balance) as gross_loans,
        sum(allowance_amt) as total_allowance,
        sum(case when days_past_due >= 90 then current_balance else 0 end) as npl_balance
    from joined
    group by account_type
)

select
    '{{ sas_report_month() }}' as report_month,
    account_type,
    n_loans,
    gross_loans,
    total_allowance,
    case when gross_loans > 0 then total_allowance / gross_loans * 100 else 0 end as coverage_pct,
    npl_balance,
    case when npl_balance > 0 then total_allowance / npl_balance * 100 else 0 end as npl_coverage_pct,
    {{ format_account_type('account_type') }} as account_type_desc
from aggregated
