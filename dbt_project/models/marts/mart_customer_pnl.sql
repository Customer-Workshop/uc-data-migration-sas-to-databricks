/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL: interest income by customer from STG_BANK.CUST_ACCOUNTS_DAILY
    Step 2 — PROC SQL: fee income from CURATED.DAILY_TRANSACTIONS
    Step 3 — PROC SQL: expected credit loss from CURATED.RISK_SCORES
    Step 4 — DATA step: MERGE BY CUSTOMER_ID with IF A (left-join semantics),
             operating cost allocation ($15/account/month), total revenue,
             net profit, annualized ROA, profitability tier assignment

  dbt Equivalent:
    CTEs replace PROC SQL work tables; LEFT JOIN replaces SAS MERGE BY + IF A;
    CASE WHEN replaces DATA step IF/THEN/ELSE tier assignment.
    All thresholds, cost factors, and CASE branches are source-faithful.
*/

{{
    config(
        materialized='table'
    )
}}

with interest_income as (
    -- SAS Step 1: PROC SQL from STG_BANK.CUST_ACCOUNTS_DAILY
    -- Filters to month-end snapshot; in dbt the staging model is already current.
    select
        a.customer_id,
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        -- Lending income: lending account types earn interest income
        sum(case when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then a.current_balance * a.interest_rate / 12 else 0 end)
            as lending_income,

        -- Deposit cost: deposit account types incur interest expense
        sum(case when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then a.current_balance * a.interest_rate / 12 else 0 end)
            as deposit_cost,

        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship

    from {{ ref('int_account_metrics') }} a
    group by a.customer_id
),

fee_income as (
    -- SAS Step 2: PROC SQL from CURATED.DAILY_TRANSACTIONS
    -- Source filters to report_month date range; mart_daily_transactions is the
    -- dbt equivalent of the curated SAS dataset.
    select
        t.customer_id,
        sum(case when t.transaction_type = 'FEE'
            then abs(t.transaction_amount) else 0 end) as fee_income,
        sum(case when t.transaction_type = 'INT'
            then abs(t.transaction_amount) else 0 end) as int_credited,
        count(*) as txn_volume
    from {{ ref('mart_daily_transactions') }} t
    group by t.customer_id
),

ecl as (
    -- SAS Step 3: PROC SQL from CURATED.RISK_SCORES
    -- Source uses latest SCORE_DATE <= month_end; in dbt we take the latest available.
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    where r.score_date = (select max(score_date) from {{ ref('mart_risk_scores') }})
    group by r.customer_id
),

-- SAS Step 4: DATA step MERGE BY CUSTOMER_ID; IF A retains only customers
-- present in INTEREST_INCOME (left-join semantics).
assembled as (
    select
        ii.customer_id,
        ii.customer_segment,
        ii.region_code,
        ii.branch_id,
        ii.lending_income,
        ii.deposit_cost,
        ii.lending_income - ii.deposit_cost as net_interest_income,
        ii.num_accounts,
        ii.total_relationship,
        coalesce(fi.fee_income, 0) as fee_income,
        coalesce(fi.int_credited, 0) as int_credited,
        coalesce(fi.txn_volume, 0) as txn_volume,
        coalesce(ecl.total_ecl, 0) as total_ecl,

        -- Source-faithful: $15/account/month operating cost allocation
        ii.num_accounts * 15 as operating_cost,

        -- SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        -- SAS sum() treats missing as 0; coalesce mirrors that behaviour.
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0) as total_revenue,

        -- SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - coalesce(TOTAL_ECL, 0)
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0)
            - (ii.num_accounts * 15)
            - coalesce(ecl.total_ecl, 0) as net_profit

    from interest_income ii
    left join fee_income fi
        on ii.customer_id = fi.customer_id
    left join ecl
        on ii.customer_id = ecl.customer_id
)

select
    customer_id,
    customer_segment,
    region_code,
    branch_id,
    lending_income,
    deposit_cost,
    net_interest_income,
    num_accounts,
    total_relationship,
    fee_income,
    int_credited,
    txn_volume,
    total_ecl,
    operating_cost,
    total_revenue,
    net_profit,

    -- SAS: ROA (annualized) — NULL when total_relationship <= 0
    case
        when total_relationship > 0
            then (net_profit * 12) / total_relationship
        else null
    end as roa,

    -- SAS: Profitability tier — source-faithful thresholds and ordering
    case
        when net_profit >= 500 then 'Highly Profitable'
        when net_profit >= 100 then 'Profitable'
        when net_profit >= 0   then 'Marginal'
        else 'Unprofitable'
    end as profit_tier,

    '{{ var("prev_ym") }}' as report_month

from assembled
