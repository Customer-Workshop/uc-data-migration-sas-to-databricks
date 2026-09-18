/*
  Reconciliation test: exception detail parity (load_customer_accounts.sas, Step 2).

  Per-row, value-for-value parity: every (ACCOUNT_ID, EXCEPTION_CODE,
  EXCEPTION_DESC) the SAS rules produce from the raw source must exist in the
  model, and the model must contain nothing else. This is what catches a rule
  that fires on the right *number* of accounts but the wrong ones, or a message
  built from the wrong field.

  The expected messages rebuild the source's catx()/put() expressions:
    NEG_BAL    catx(' ','Negative balance', put(CURRENT_BALANCE, dollar18.2),
                    'on deposit account', ACCOUNT_ID)
    HIGH_UTIL  catx(' ','Utilization at', put(UTILIZATION_PCT, 5.1), '%',
                    'for account', ACCOUNT_ID)
    NO_RISK    catx(' ','Missing risk rating for customer', CUSTOMER_ID)

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with in_scope as (
    select
        a.account_id,
        a.customer_id,
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
        account_id,
        'NEG_BAL' as exception_code,
        concat_ws(
            ' ',
            'Negative balance',
            concat('-$', format_number(abs(current_balance), 2)),
            'on deposit account',
            account_id
        ) as exception_desc
    from in_scope
    where account_type in ('CHK', 'SAV', 'MMA', 'CD') and current_balance < 0
    union all
    select
        account_id,
        'HIGH_UTIL' as exception_code,
        concat_ws(
            ' ',
            'Utilization at',
            format_number((current_balance / credit_limit) * 100, 1),
            '%',
            'for account',
            account_id
        ) as exception_desc
    from in_scope
    where account_type in ('CC', 'LOC', 'HELC')
      and credit_limit > 0
      and (current_balance / credit_limit) * 100 > 95
    union all
    select
        account_id,
        'NO_RISK' as exception_code,
        concat_ws(' ', 'Missing risk rating for customer', customer_id) as exception_desc
    from in_scope
    where risk_rating is null
),

actual as (
    select
        account_id,
        exception_code,
        exception_desc
    from {{ ref('int_account_exceptions') }}
),

missing_in_model as (
    select
        'missing_in_model' as side,
        account_id,
        exception_code,
        exception_desc
    from (select * from expected except all select * from actual)
),

extra_in_model as (
    select
        'extra_in_model' as side,
        account_id,
        exception_code,
        exception_desc
    from (select * from actual except all select * from expected)
)

select * from missing_in_model
union all
select * from extra_in_model
