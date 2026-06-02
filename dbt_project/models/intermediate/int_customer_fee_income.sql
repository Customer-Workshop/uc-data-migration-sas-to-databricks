/*
  int_customer_fee_income.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 2: WORK.FEE_INCOME)

  SAS Original:
    PROC SQL ... GROUP BY t.CUSTOMER_ID over CURATED.DAILY_TRANSACTIONS where
    TRANSACTION_DATE between "&month_start"d and "&month_end"d. Sums fee income,
    interest credited, and counts transaction volume per customer.

  dbt Equivalent:
    PROC SQL GROUP BY -> dbt model with GROUP BY over mart_daily_transactions
    (the converted CURATED.DAILY_TRANSACTIONS, which carries customer_id from the
    enrichment join).

  SAS: where TRANSACTION_DATE between "&month_start"d and "&month_end"d
    The curated feed represents exactly the reporting period's transactions (the
    daily batch populates CURATED.DAILY_TRANSACTIONS for the period). The seed
    materialises one ~30-day period, so no additional month-window filter is
    applied here — the curated feed is already the period. Flagged for visibility.
*/

with txns as (
    select * from {{ ref('mart_daily_transactions') }}
)

select
    customer_id,

    -- SAS: FEE_INCOME = sum(case when TRANSACTION_TYPE = 'FEE' then abs(TRANSACTION_AMOUNT) else 0)
    sum(
        case when transaction_type = 'FEE' then abs(transaction_amount) else 0 end
    ) as fee_income,

    -- SAS: INT_CREDITED = sum(case when TRANSACTION_TYPE = 'INT' then abs(TRANSACTION_AMOUNT) else 0)
    sum(
        case when transaction_type = 'INT' then abs(transaction_amount) else 0 end
    ) as int_credited,

    -- SAS: TXN_VOLUME = count(*)
    count(*) as txn_volume

from txns
-- Orphan transactions (no enriched customer_id) are not attributable to a
-- customer; SAS's curated feed always carries customer_id, so exclude nulls.
where customer_id is not null
group by customer_id
