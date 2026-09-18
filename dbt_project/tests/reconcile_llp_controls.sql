/*
  monthly_regulatory_reporting.sas Step 3 (LLP_COVERAGE)
    - completeness: sum(N_LOANS) = lending accounts with a LOAN_DETAILS row
    - control totals: GROSS_LOANS / TOTAL_ALLOWANCE / NPL_BALANCE re-derived
    - percentage identities with the divide guards (0 when denominator = 0)
*/
with base as (
    select
        a.account_type,
        count(*) as n_loans,
        sum(a.current_balance) as gross_loans,
        sum(l.allowance_amt) as total_allowance,
        sum(case when l.days_past_due >= 90 then a.current_balance else 0 end) as npl_balance
    from {{ ref('int_account_metrics') }} a
    inner join {{ ref('stg_loan_details') }} l on a.account_id = l.account_id
    where a.snapshot_date = {{ sas_month_end() }}
      and a.account_type in {{ sas_lending_products() }}
    group by a.account_type
),

report as (
    select * from {{ ref('mart_llp_coverage') }}
)

select
    coalesce(b.account_type, r.account_type) as account_type,
    case
        when r.account_type is null then 'missing in report'
        when b.account_type is null then 'unexpected in report'
        when b.n_loans <> r.n_loans then 'n_loans'
        when not {{ approx_equal('b.gross_loans', 'r.gross_loans') }} then 'gross_loans'
        when not {{ approx_equal('b.total_allowance', 'r.total_allowance') }} then 'total_allowance'
        when not {{ approx_equal('b.npl_balance', 'r.npl_balance') }} then 'npl_balance'
        when not {{ approx_equal_rel('r.coverage_pct', 'case when r.gross_loans > 0 then r.total_allowance / r.gross_loans * 100 else 0 end') }} then 'coverage_pct'
        when not {{ approx_equal_rel('r.npl_coverage_pct', 'case when r.npl_balance > 0 then r.total_allowance / r.npl_balance * 100 else 0 end') }} then 'npl_coverage_pct'
    end as issue
from base b
full outer join report r on b.account_type = r.account_type
where b.account_type is null or r.account_type is null
   or b.n_loans <> r.n_loans
   or not {{ approx_equal('b.gross_loans', 'r.gross_loans') }}
   or not {{ approx_equal('b.total_allowance', 'r.total_allowance') }}
   or not {{ approx_equal('b.npl_balance', 'r.npl_balance') }}
   or not {{ approx_equal_rel('r.coverage_pct', 'case when r.gross_loans > 0 then r.total_allowance / r.gross_loans * 100 else 0 end') }}
   or not {{ approx_equal_rel('r.npl_coverage_pct', 'case when r.npl_balance > 0 then r.total_allowance / r.npl_balance * 100 else 0 end') }}
