/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL: Interest income by customer (lending income,
    deposit cost, net interest income) from STG_BANK.CUST_ACCOUNTS_DAILY.
    Step 2 — PROC SQL: Fee income from CURATED.DAILY_TRANSACTIONS.
    Step 3 — PROC SQL: Expected credit loss from CURATED.RISK_SCORES.
    Step 4 — DATA step MERGE BY: Assemble customer P&L with operating
    cost allocation, ROA, and profitability tier.

  dbt Equivalent:
    Multi-ref LEFT JOIN replaces SAS multi-source MERGE BY.
    SQL CASE expressions replace DATA step IF/THEN tier assignment.
    dbt var('prev_ym') replaces SAS &PREV_YM macro variable.
*/

-- SAS: Step 1 — Interest Income by Customer
with interest_income as (
    select
        customer_id,
        max(customer_segment)  as customer_segment,
        max(region_code)       as region_code,
        max(branch_id)         as branch_id,

        -- Lending income
        sum(case
            when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then current_balance * interest_rate / 12
            else 0
        end) as lending_income,

        -- Deposit cost
        sum(case
            when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then current_balance * interest_rate / 12
            else 0
        end) as deposit_cost,

        count(distinct account_id)  as num_accounts,
        sum(current_balance)        as total_relationship

    from {{ ref('int_account_metrics') }}
    group by customer_id
),

-- SAS: Step 2 — Fee Income from Transactions
fee_income as (
    select
        customer_id,
        sum(case
            when transaction_type = 'FEE'
            then abs(transaction_amount) else 0
        end) as fee_income,
        sum(case
            when transaction_type = 'INT'
            then abs(transaction_amount) else 0
        end) as int_credited,
        count(*) as txn_volume
    from {{ ref('mart_daily_transactions') }}
    group by customer_id
),

-- SAS: Step 3 — Expected Credit Loss by Customer
ecl as (
    select
        customer_id,
        sum(expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }}
    group by customer_id
),

-- SAS: Step 4 — Customer P&L Assembly (MERGE BY)
assembled as (
    select
        i.customer_id,
        i.customer_segment,
        i.region_code,
        i.branch_id,
        i.num_accounts,
        i.total_relationship,

        -- Net Interest Income
        i.lending_income - i.deposit_cost as net_interest_income,

        coalesce(f.fee_income, 0)   as fee_income,
        coalesce(f.int_credited, 0) as int_credited,
        coalesce(f.txn_volume, 0)   as txn_volume,

        -- Operating cost allocation ($15/account/month)
        i.num_accounts * 15 as operating_cost,

        -- Total Revenue
        (i.lending_income - i.deposit_cost) + coalesce(f.fee_income, 0)
            as total_revenue,

        coalesce(e.total_ecl, 0) as total_ecl,

        -- Net Profit
        (i.lending_income - i.deposit_cost)
            + coalesce(f.fee_income, 0)
            - (i.num_accounts * 15)
            - coalesce(e.total_ecl, 0)
        as net_profit,

        -- ROA (annualized)
        case
            when i.total_relationship > 0
            then (
                (i.lending_income - i.deposit_cost)
                + coalesce(f.fee_income, 0)
                - (i.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) * 12 / i.total_relationship
            else null
        end as roa,

        -- SAS: Profitability tier
        case
            when (
                (i.lending_income - i.deposit_cost)
                + coalesce(f.fee_income, 0)
                - (i.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 500 then 'Highly Profitable'
            when (
                (i.lending_income - i.deposit_cost)
                + coalesce(f.fee_income, 0)
                - (i.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 100 then 'Profitable'
            when (
                (i.lending_income - i.deposit_cost)
                + coalesce(f.fee_income, 0)
                - (i.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 0 then 'Marginal'
            else 'Unprofitable'
        end as profit_tier,

        {{ var('prev_ym') }} as report_month

    from interest_income i
    left join fee_income f
        on i.customer_id = f.customer_id
    left join ecl e
        on i.customer_id = e.customer_id
)

select * from assembled
