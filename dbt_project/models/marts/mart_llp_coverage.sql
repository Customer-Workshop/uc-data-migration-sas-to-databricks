/*
  mart_llp_coverage.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 3)

  SAS Original:
    PROC SQL creating REPORTS.LLP_COVERAGE from STG_BANK.CUST_ACCOUNTS_DAILY
    INNER joined to ORA_DW.LOAN_DETAILS (so only accounts with a loan record
    count), grouped by REPORT_MONTH / ACCOUNT_TYPE.

  dbt Equivalent:
    Aggregates and guarded ratio CASEs reproduced as written; NPL_COVERAGE_PCT
    keeps the SAS definition (total allowance over NPL balance).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
      and account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

joined as (
    select
        a.account_type,
        a.current_balance,
        l.allowance_amt,
        l.days_past_due
    from accounts a
    inner join loan_details l
        on a.account_id = l.account_id
),

aggregated as (
    select
        account_type,
        count(*) as n_loans,
        sum(current_balance) as gross_loans,
        sum(allowance_amt) as total_allowance,
        -- Source-faithful: a missing DAYS_PAST_DUE never satisfies >= 90 in SAS,
        -- so those balances stay out of NPL_BALANCE.
        sum(case when days_past_due >= 90 then current_balance else 0 end)
            as npl_balance
    from joined
    group by account_type
)

select
    {{ sas_report_month() }} as report_month,
    account_type,
    n_loans,
    gross_loans,
    total_allowance,
    npl_balance,
    case
        when gross_loans > 0 then total_allowance / gross_loans * 100
        else 0
    end as coverage_pct,
    -- Source-faithful: the SAS ratio divides TOTAL allowance (not just the
    -- allowance held against non-performing loans) by the NPL balance.
    case
        when npl_balance > 0 then total_allowance / npl_balance * 100
        else 0
    end as npl_coverage_pct
from aggregated
order by account_type
