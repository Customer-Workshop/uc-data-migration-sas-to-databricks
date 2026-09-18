/*
  Reconciliation test: per-exception-code parity (load_customer_accounts.sas, Step 2).

  A total row count can tie out while an individual rule is wrong (one rule
  over-firing and another under-firing cancel out). This control compares the
  model's count per EXCEPTION_CODE against the same rule evaluated on the raw
  source, code by code, including codes the model emits that the SAS program
  has no rule for (those get an expected count of 0 and fail).

  SAS rules (Step 2 of the DATA step, in emission order):
    NEG_BAL    ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') and CURRENT_BALANCE < 0
    HIGH_UTIL  UTILIZATION_PCT > 95, where UTILIZATION_PCT is
               (CURRENT_BALANCE / CREDIT_LIMIT) * 100 for ACCOUNT_TYPE in
               ('CC','LOC','HELC') with CREDIT_LIMIT > 0, otherwise missing
    NO_RISK    RISK_RATING = .  (numeric missing)

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
    select 'NEG_BAL' as exception_code, count(*) as n
    from in_scope
    where account_type in ('CHK', 'SAV', 'MMA', 'CD') and current_balance < 0
    union all
    select 'HIGH_UTIL' as exception_code, count(*) as n
    from in_scope
    where account_type in ('CC', 'LOC', 'HELC')
      and credit_limit > 0
      and (current_balance / credit_limit) * 100 > 95
    union all
    select 'NO_RISK' as exception_code, count(*) as n
    from in_scope
    where risk_rating is null
),

actual as (
    select
        exception_code,
        count(*) as n
    from {{ ref('int_account_exceptions') }}
    group by exception_code
)

select
    coalesce(e.exception_code, a.exception_code) as exception_code,
    coalesce(e.n, 0) as sas_rule_rows,
    coalesce(a.n, 0) as model_rows
from expected e
full outer join actual a on e.exception_code = a.exception_code
where coalesce(e.n, 0) <> coalesce(a.n, 0)
