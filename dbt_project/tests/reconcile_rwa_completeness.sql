/*
  Reconciliation test: REPORTS.MONTHLY_RWA completeness.
  Program: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  The SAS step reads every row of STG_BANK.CUST_ACCOUNTS_DAILY at the month-end
  snapshot and LEFT joins ORA_DW.LOAN_DETAILS, so the report's N_ACCOUNTS must
  add up to the whole in-scope account population — no dropped rows, and no
  fan-out from the loan join (LOAN_DETAILS holds at most one row per account).

  The expected population is derived from the raw source with the extract's
  documented scope rule (load_customer_accounts.sas Step 1), not from another
  converted model, so a conversion error upstream cannot hide here.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= {{ sas_run_date() }}
),

reported as (
    select sum(n_accounts) as n from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_in_scope_accounts,
    r.n as reported_accounts,
    r.n - e.n as difference
from expected_in_scope e
cross join reported r
where e.n <> r.n
