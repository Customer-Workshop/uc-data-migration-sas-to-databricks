/*
  Reconciliation control: transaction amount control total.

  DAILY_TRANSACTIONS.SUM_AMOUNT in the SAS golden controls is the unsigned sum
  of TRANSACTION_AMOUNT over the curated table. It ties out only if the model
  holds exactly the in-scope rows and carries amounts through unchanged (no
  sign flip, no currency conversion, no rounding).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select round(
        (
            select coalesce(sum(transaction_amount), 0)
            from {{ source('banking_raw', 'curated_daily_transactions_history') }}
        )
        + (
            select coalesce(sum(transaction_amount), 0)
            from {{ source('banking_raw', 'daily_transactions') }}
            where transaction_id is not null and trim(transaction_id) <> ''
              and account_id is not null and trim(account_id) <> ''
              and transaction_amount is not null
              and abs(transaction_amount) <= 10000000
              and transaction_type in (
                  'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
              )
              and transaction_date <= {{ sas_run_date() }}
        ), 2) as amt
),

actual as (
    select round(coalesce(sum(transaction_amount), 0), 2) as amt
    from {{ ref('mart_daily_transactions') }}
)

select
    e.amt as expected_sum_amount,
    a.amt as model_sum_amount,
    a.amt - e.amt as difference
from expected e
cross join actual a
where abs(a.amt - e.amt) > 0.01
