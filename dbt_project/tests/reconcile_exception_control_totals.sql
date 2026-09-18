/*
  Reconciliation test: exception control totals (load_customer_accounts.sas, Step 2).

  Row counts prove the right number of exceptions; these sums prove they are the
  right exceptions. Two totals, recomputed from the raw source with the Step 1
  extract filter:

    NEG_BAL    sum of CURRENT_BALANCE over the negative-balance deposit accounts
               (the overdrawn exposure the exception report exists to surface)
    HIGH_UTIL  sum of UTILIZATION_PCT over the over-95% revolving accounts

  Compared to the cent / to two decimals, so a wrong account picked up and a
  right one dropped cannot cancel out.

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
        round(
            sum(
                case
                    when account_type in ('CHK', 'SAV', 'MMA', 'CD') and current_balance < 0
                        then current_balance
                end
            ), 2
        ) as neg_bal_total,
        round(
            sum(
                case
                    when account_type in ('CC', 'LOC', 'HELC')
                         and credit_limit > 0
                         and (current_balance / credit_limit) * 100 > 95
                        then (current_balance / credit_limit) * 100
                end
            ), 2
        ) as high_util_total
    from in_scope
),

actual as (
    select
        round(sum(case when exception_code = 'NEG_BAL' then current_balance end), 2)
            as neg_bal_total,
        round(sum(case when exception_code = 'HIGH_UTIL' then utilization_pct end), 2)
            as high_util_total
    from {{ ref('int_account_exceptions') }}
)

select
    e.neg_bal_total as expected_neg_bal_total,
    a.neg_bal_total as model_neg_bal_total,
    e.high_util_total as expected_high_util_total,
    a.high_util_total as model_high_util_total
from expected e
cross join actual a
where e.neg_bal_total is distinct from a.neg_bal_total
   or e.high_util_total is distinct from a.high_util_total
