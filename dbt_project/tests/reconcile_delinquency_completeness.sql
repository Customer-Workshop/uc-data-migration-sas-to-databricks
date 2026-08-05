/*
  Reconciliation control: delinquency aging completeness.

  monthly_regulatory_reporting.sas Step 2 narrows the month-end snapshot to the
  six lending account types and LEFT JOINs LOAN_DETAILS, so every in-scope
  lending account appears exactly once — including accounts with no LOAN_DETAILS
  row, which the source parks in the 'Unknown' bucket rather than dropping.

  This control proves the scope contract: no lending account lost (an INNER join
  would silently drop the ones without loan rows) and no fan-out.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_accounts as (
    select sum(n_accounts) as n from {{ ref('mart_delinquency_aging') }}
)

select
    e.n as expected_in_scope_accounts,
    m.n as mart_accounts,
    m.n - e.n as difference
from expected_in_scope e
cross join mart_accounts m
where e.n <> m.n
