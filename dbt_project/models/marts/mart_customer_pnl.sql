/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL → WORK.INTEREST_INCOME (interest income by customer)
    Step 2: PROC SQL → WORK.FEE_INCOME (fee income from transactions)
    Step 3: PROC SQL → WORK.ECL (expected credit loss)
    Step 4: DATA step MERGE BY CUSTOMER_ID (IF a → keep interest_income customers)
            Computes operating_cost, total_revenue, net_profit, ROA, profit_tier

  dbt Equivalent:
    CTEs replace WORK tables, LEFT JOINs replace SAS MERGE BY with IF a,
    CASE WHEN replaces DATA step IF/THEN tier assignment,
    SAS sum(x, y, 0) (treats missing as 0) → coalesce(x, 0) + coalesce(y, 0)
*/

with interest_income as (
    -- SAS Step 1: Interest Income by Customer
    -- Source: STG_BANK.CUST_ACCOUNTS_DAILY WHERE SNAPSHOT_DATE = month_end
    -- In dbt: stg_cust_accounts already filters to active accounts (latest data)
    select
        customer_id,
        max(customer_segment) as customer_segment,
        max(region_code) as region_code,
        max(branch_id) as branch_id,
        -- Lending income: account_type in (MTG, AUTO, PERS, CC, LOC, HELC)
        sum(
            case
                when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as lending_income,
        -- Deposit cost: account_type in (CHK, SAV, MMA, CD, IRA)
        sum(
            case
                when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as deposit_cost,
        -- Net interest income = lending - deposit cost
        sum(
            case
                when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) - sum(
            case
                when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as net_interest_income,
        count(distinct account_id) as num_accounts,
        sum(current_balance) as total_relationship
    from {{ ref('stg_cust_accounts') }}
    group by customer_id
),

fee_income as (
    -- SAS Step 2: Fee Income from Transactions
    -- Source: CURATED.DAILY_TRANSACTIONS filtered to reporting month
    -- In dbt: mart_daily_transactions is the equivalent; filter to prev month
    select
        customer_id,
        sum(
            case
                when transaction_type = 'FEE' then abs(transaction_amount)
                else 0
            end
        ) as fee_income,
        sum(
            case
                when transaction_type = 'INT' then abs(transaction_amount)
                else 0
            end
        ) as int_credited,
        count(*) as txn_volume
    from {{ ref('mart_daily_transactions') }}
    where transaction_date >= date_trunc('month', add_months(current_date(), -1))
      and transaction_date < date_trunc('month', current_date())
    group by customer_id
),

ecl as (
    -- SAS Step 3: Expected Credit Loss by Customer
    -- Source: CURATED.RISK_SCORES WHERE SCORE_DATE = max(SCORE_DATE <= month_end)
    -- In dbt: mart_risk_scores uses score_date = current_date() for latest
    select
        customer_id,
        sum(expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }}
    where score_date = current_date()
    group by customer_id
),

-- SAS Step 4: Customer P&L Assembly
-- SAS: MERGE BY CUSTOMER_ID; IF a → keep only customers from interest_income
customer_pnl as (
    select
        ii.customer_id,
        ii.customer_segment,
        ii.region_code,
        ii.branch_id,
        ii.lending_income,
        ii.deposit_cost,
        ii.net_interest_income,
        ii.num_accounts,
        ii.total_relationship,
        fi.fee_income,
        fi.int_credited,
        fi.txn_volume,
        e.total_ecl,
        -- Operating cost: $15 per account per month (source-faithful)
        ii.num_accounts * 15 as operating_cost,
        -- SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        -- sum() in SAS treats missing as 0
        coalesce(ii.net_interest_income, 0) + coalesce(fi.fee_income, 0)
            as total_revenue,
        -- Net Profit
        (coalesce(ii.net_interest_income, 0) + coalesce(fi.fee_income, 0))
            - (ii.num_accounts * 15)
            - coalesce(e.total_ecl, 0) as net_profit,
        -- ROA (annualized): null if total_relationship = 0
        case
            when ii.total_relationship > 0
                then (
                    (coalesce(ii.net_interest_income, 0)
                        + coalesce(fi.fee_income, 0))
                    - (ii.num_accounts * 15)
                    - coalesce(e.total_ecl, 0)
                ) * 12 / ii.total_relationship
            else null
        end as roa,
        -- Profit tier: source-faithful IF/THEN cascade
        case
            when (coalesce(ii.net_interest_income, 0)
                + coalesce(fi.fee_income, 0))
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 500
                then 'Highly Profitable'
            when (coalesce(ii.net_interest_income, 0)
                + coalesce(fi.fee_income, 0))
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 100
                then 'Profitable'
            when (coalesce(ii.net_interest_income, 0)
                + coalesce(fi.fee_income, 0))
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 0
                then 'Marginal'
            else 'Unprofitable'
        end as profit_tier,
        '{{ var("prev_ym") }}' as report_month
    from interest_income ii
    left join fee_income fi on ii.customer_id = fi.customer_id
    left join ecl e on ii.customer_id = e.customer_id
)

select * from customer_pnl
