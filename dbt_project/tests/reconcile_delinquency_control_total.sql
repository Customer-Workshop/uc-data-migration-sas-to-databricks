/*
  Reconciliation control: delinquency aging control totals.

  TOTAL_BALANCE and TOTAL_PAST_DUE must tie back to the source for the in-scope
  lending population. SAS SUM() ignores missing values, so a lending account
  with no LOAN_DETAILS row contributes its balance but nothing to past due; the
  SQL SUM behaves the same way and this control pins that behaviour.

  Tolerance is one cent, for floating-point summation order only.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_totals as (
    select
        sum(a.current_balance) as total_balance,
        sum(l.past_due_amount) as total_past_due
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
      and a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart_totals as (
    select
        sum(total_balance) as total_balance,
        sum(total_past_due) as total_past_due
    from {{ ref('mart_delinquency_aging') }}
),

checks as (
    select
        'total_balance' as control,
        s.total_balance as expected_value,
        m.total_balance as actual_value
    from source_totals s
    cross join mart_totals m

    union all

    select
        'total_past_due' as control,
        s.total_past_due as expected_value,
        m.total_past_due as actual_value
    from source_totals s
    cross join mart_totals m
)

select
    control,
    expected_value,
    actual_value,
    actual_value - expected_value as difference
from checks
where abs(actual_value - expected_value) > 0.01
