/*
  Control total: the SAS RETAIN chain. For every account the last
  RUNNING_BALANCE (in ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID order) must
  equal PRE_TXN_BALANCE + sum of the per-transaction balance effects, and
  every step must move by exactly one transaction's effect.
*/
with txns as (
    select
        account_id,
        transaction_id,
        pre_txn_balance,
        running_balance,
        {{ sas_txn_balance_effect('transaction_type', 'transaction_amount') }} as effect,
        lag(running_balance) over (
            partition by account_id order by transaction_date, transaction_id
        ) as prev_running_balance
    from {{ ref('mart_daily_transactions') }}
    where transaction_date = {{ sas_run_date() }}
),

step_check as (
    select
        account_id,
        transaction_id,
        'step' as control,
        coalesce(prev_running_balance, pre_txn_balance) + effect as expected,
        running_balance as actual
    from txns
    where pre_txn_balance is not null
),

total_check as (
    select
        account_id,
        cast(null as string) as transaction_id,
        'account total' as control,
        max(pre_txn_balance) + sum(effect) as expected,
        max_by(running_balance, transaction_id) as actual
    from txns
    where pre_txn_balance is not null
    group by account_id
)

select * from step_check where not {{ approx_equal('expected', 'actual') }}
union all
select * from total_check where not {{ approx_equal('expected', 'actual') }}
