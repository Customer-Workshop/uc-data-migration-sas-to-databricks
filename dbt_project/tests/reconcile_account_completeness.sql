/*
  Reconciliation test: account completeness against the documented extract scope.

  The SAS extract (load_customer_accounts.sas, Step 1) did not carry every raw
  account forward — it joined CUST_ACCOUNTS to CUST_DEMOGRAPHICS and applied a
  WHERE that excluded writeoff/closed accounts. The dbt staging model
  (stg_cust_accounts) reproduces that contract:
      inner join cust_demographics on customer_id
      where account_status not in ('W','C') and open_date <= current_date()

  A naive "raw count == model count" check fails here (and should) because the
  conversion legitimately drops out-of-scope accounts. A meaningful control
  reconciles the model against the *expected in-scope population* — proving the
  conversion dropped exactly the rows the business rule says to drop, no more and
  no fewer (e.g. a fanned-out join or an over-broad filter would surface here).

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

model_accounts as (
    select count(*) as n from {{ ref('int_account_metrics') }}
)

select
    e.n as expected_in_scope_accounts,
    m.n as model_accounts,
    m.n - e.n as difference
from expected_in_scope e
cross join model_accounts m
where e.n <> m.n
