/*
  Reconciliation test: REPORTS.LLP_COVERAGE parity.
  Program: Programs/Banking/monthly_regulatory_reporting.sas (Step 3)

  The SAS step INNER joins STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS,
  so only accounts that actually carry a loan record are counted. Every measure
  is re-derived from the source here and compared value-for-value per account
  type: loan counts, gross loans, allowance, NPL balance (DAYS_PAST_DUE >= 90)
  and both coverage ratios.

  Source-faithful: NPL_COVERAGE_PCT divides the TOTAL allowance by the NPL
  balance (not just the allowance held against non-performing loans), and both
  ratios fall back to 0 when the denominator is not positive.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_population as (
    select
        a.account_type,
        a.current_balance,
        l.allowance_amt,
        l.days_past_due
    from {{ ref('int_account_metrics') }} a
    inner join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.snapshot_date = {{ sas_month_end() }}
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

expected as (
    select
        account_type,
        count(*) as n_loans,
        round(sum(current_balance), 2) as gross_loans,
        round(sum(allowance_amt), 2) as total_allowance,
        round(
            sum(case when days_past_due >= 90 then current_balance else 0 end), 2
        ) as npl_balance
    from source_population
    group by account_type
),

reported as (
    select
        account_type,
        n_loans,
        round(gross_loans, 2) as gross_loans,
        round(total_allowance, 2) as total_allowance,
        round(npl_balance, 2) as npl_balance,
        coverage_pct,
        npl_coverage_pct
    from {{ ref('mart_llp_coverage') }}
)

select
    coalesce(e.account_type, r.account_type) as account_type,
    e.n_loans as sas_n_loans,
    r.n_loans as mart_n_loans,
    e.gross_loans as sas_gross_loans,
    r.gross_loans as mart_gross_loans,
    e.total_allowance as sas_total_allowance,
    r.total_allowance as mart_total_allowance,
    e.npl_balance as sas_npl_balance,
    r.npl_balance as mart_npl_balance
from expected e
full outer join reported r
    on e.account_type = r.account_type
where e.account_type is null
   or r.account_type is null
   or e.n_loans <> r.n_loans
   or abs(e.gross_loans - r.gross_loans) > 0.01
   or abs(e.total_allowance - r.total_allowance) > 0.01
   or abs(e.npl_balance - r.npl_balance) > 0.01
   or abs(
        case when e.gross_loans > 0
            then e.total_allowance / e.gross_loans * 100 else 0
        end - r.coverage_pct
      ) > 0.01
   or abs(
        case when e.npl_balance > 0
            then e.total_allowance / e.npl_balance * 100 else 0
        end - r.npl_coverage_pct
      ) > 0.01
