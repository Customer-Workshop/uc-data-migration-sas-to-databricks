/*
  Reconciliation control: RWA completeness (no row loss, no join fan-out).

  monthly_regulatory_reporting.sas Step 1 reads the whole month-end account
  snapshot (no account-type filter) and LEFT JOINs LOAN_DETAILS, so the account
  count behind REPORTS.MONTHLY_RWA equals the in-scope account population — one
  row per account, no more. If the LOAN_DETAILS join fanned out (duplicate
  account_id) the exposure totals would silently inflate; if a filter crept in,
  accounts would silently disappear. Both surface here.

  In-scope population = the documented extract contract of
  load_customer_accounts.sas (see reconcile_account_completeness.sql).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
),

mart_accounts as (
    select sum(n_accounts) as n from {{ ref('mart_regulatory_rwa') }}
)

select
    e.n as expected_in_scope_accounts,
    m.n as mart_accounts,
    m.n - e.n as difference
from expected_in_scope e
cross join mart_accounts m
where e.n <> m.n
