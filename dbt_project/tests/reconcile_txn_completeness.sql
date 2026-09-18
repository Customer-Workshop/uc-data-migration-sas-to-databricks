/*
  Completeness: every feed row lands in exactly one branch.

  SAS daily_transaction_processing.sas Step 1 splits RAW_BANK.TXN_FEED into
  WORK.TXN_VALIDATED and WORK.TXN_REJECTED. This control proves
      feed = validated + rejected           (no row lost or duplicated)
      curated = history + validated         (PROC APPEND shape preserved)
      running_balances = validated          (DATA step KEEP, one row per txn)
*/
with feed as (
    select count(*) as n, sum(transaction_amount) as amt
    from {{ source('banking_raw', 'daily_transactions') }}
),

validated as (
    select count(*) as n, sum(transaction_amount) as amt
    from {{ ref('stg_daily_transactions') }}
),

rejected as (
    select count(*) as n, sum(transaction_amount) as amt
    from {{ ref('stg_txn_rejected') }}
),

history as (
    select count(*) as n
    from {{ source('banking_raw', 'curated_daily_transactions_history') }}
),

curated as (
    select count(*) as n from {{ ref('mart_daily_transactions_curated') }}
),

balances as (
    select count(*) as n from {{ ref('mart_running_balances') }}
),

checks as (
    select
        'feed = validated + rejected (rows)' as control,
        f.n as expected,
        v.n + r.n as actual
    from feed f, validated v, rejected r
    union all
    select
        'feed = validated + rejected (sum amount)',
        round(f.amt, 2),
        round(coalesce(v.amt, 0) + coalesce(r.amt, 0), 2)
    from feed f, validated v, rejected r
    union all
    select
        'curated = history + validated (rows)',
        h.n + v.n,
        c.n
    from history h, validated v, curated c
    union all
    select
        'running_balances = validated (rows)',
        v.n,
        b.n
    from validated v, balances b
)

select * from checks
where not (expected <=> actual)
