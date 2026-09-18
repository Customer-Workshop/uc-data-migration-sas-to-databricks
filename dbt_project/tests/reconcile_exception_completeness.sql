/*
  Reconciliation test: exception completeness (load_customer_accounts.sas, Step 2).

  The in-scope population for the exception branch is not "one row per account":
  the DATA step emits one row per rule that fires, so an account can contribute
  0, 1 or 2 rows. The documented scope is therefore the union of the three rule
  populations, recomputed here straight from the raw source with the same
  extract filter the program used (Step 1):

      inner join cust_demographics on customer_id
      where account_status not in ('W','C') and open_date <= {{ sas_run_date() }}

  This catches both silent row loss (a rule that stopped firing) and fan-out (a
  join that duplicated an account).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with in_scope as (
    select
        a.account_id,
        a.account_type,
        a.current_balance,
        a.credit_limit,
        d.risk_rating
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= {{ sas_run_date() }}
),

expected as (
    select
        count_if(account_type in ('CHK', 'SAV', 'MMA', 'CD') and current_balance < 0)
        + count_if(
            account_type in ('CC', 'LOC', 'HELC')
            and credit_limit > 0
            and (current_balance / credit_limit) * 100 > 95
        )
        + count_if(risk_rating is null) as n
    from in_scope
),

model_exceptions as (
    select count(*) as n from {{ ref('int_account_exceptions') }}
)

select
    e.n as expected_exception_rows,
    m.n as model_exception_rows,
    m.n - e.n as difference
from expected e
cross join model_exceptions m
where e.n <> m.n
