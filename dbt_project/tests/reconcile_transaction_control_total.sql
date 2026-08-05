/*
  Reconciliation control: CONTROL TOTAL on transaction value and balance movement.

  Source of truth: daily_transaction_processing.sas Steps 2-3. The signed balance
  movement per transaction type is:
      DEP, INT, REF, REV  ->  + TRANSACTION_AMOUNT
      WDR, PMT, FEE, CHG  ->  - abs(TRANSACTION_AMOUNT)
      TRF, ADJ            ->  + TRANSACTION_AMOUNT
      otherwise           ->  no movement

  Two totals must tie out between the raw in-scope feed and the mart:
      1. SUM(TRANSACTION_AMOUNT)  — nothing was dropped, duplicated or rescaled;
      2. SUM(signed movement)     — the type-to-direction mapping in the mart
                                    produces exactly the source's net movement.
  A per-row mapping error can cancel out in (1) but not in (2), and vice versa.

  Rounded to 2dp (the SAS dollar18.2 format) to avoid float noise.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_totals as (
    select
        round(sum(transaction_amount), 2) as total_amount,
        round(sum(
            case
                when transaction_type in ('DEP', 'INT', 'REF', 'REV')
                    then transaction_amount
                when transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                    then -abs(transaction_amount)
                when transaction_type in ('TRF', 'ADJ')
                    then transaction_amount
                else 0
            end
        ), 2) as total_movement
    from {{ source('banking_raw', 'daily_transactions') }}
    where transaction_id is not null
      and account_id is not null
      and transaction_amount is not null
      and abs(transaction_amount) <= 10000000
      and transaction_type in (
          'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
      )
      and transaction_date <= current_date()
),

mart_totals as (
    select
        round(sum(transaction_amount), 2) as total_amount,
        round(sum(post_txn_balance - pre_txn_balance), 2) as total_movement
    from {{ ref('mart_daily_transactions') }}
)

select
    s.total_amount as source_total_amount,
    m.total_amount as mart_total_amount,
    s.total_movement as source_total_movement,
    m.total_movement as mart_total_movement
from source_totals s
cross join mart_totals m
where s.total_amount <> m.total_amount
   or s.total_movement <> m.total_movement
