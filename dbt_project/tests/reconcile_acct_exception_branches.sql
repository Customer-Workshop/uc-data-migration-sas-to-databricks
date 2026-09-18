/*
  Mapping parity for load_customer_accounts.sas exception rules, both ways:
    - every exception row satisfies the rule named by its EXCEPTION_CODE
    - every snapshot account that meets a rule has exactly one row for it
*/
with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

expected as (
    select account_id, 'NEG_BAL' as exception_code from accounts
    where account_type in {{ sas_deposit_products() }} and current_balance < 0
    union all
    select account_id, 'HIGH_UTIL' from accounts where utilization_pct > 95
    union all
    select account_id, 'NO_RISK' from accounts where risk_rating is null
),

actual as (
    select account_id, exception_code, count(*) as n
    from {{ ref('int_acct_exceptions') }}
    group by account_id, exception_code
)

select
    coalesce(e.account_id, a.account_id) as account_id,
    coalesce(e.exception_code, a.exception_code) as exception_code,
    case
        when a.account_id is null then 'expected exception missing'
        when e.account_id is null then 'unexpected exception row'
        when a.n <> 1 then 'duplicate exception row'
    end as issue
from expected e
full outer join actual a
    on e.account_id = a.account_id and e.exception_code = a.exception_code
where a.account_id is null or e.account_id is null or a.n <> 1
