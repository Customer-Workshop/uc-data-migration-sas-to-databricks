/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL computing interest income by customer from
            STG_BANK.CUST_ACCOUNTS_DAILY (lending income - deposit cost)
    Step 2: PROC SQL computing fee income from CURATED.DAILY_TRANSACTIONS
    Step 3: PROC SQL computing expected credit loss from CURATED.RISK_SCORES
    Step 4: DATA step MERGE BY CUSTOMER_ID assembling P&L with
            operating cost allocation ($15/account/month) and profit tiers

  dbt Equivalent:
    CTEs replace WORK tables; LEFT JOINs replace SAS MERGE BY
    ref() calls replace SAS LIBNAME references
    SQL CASE replaces DATA step IF/THEN tier assignment
*/

with interest_income as (
    -- SAS Step 1: PROC SQL creating WORK.INTEREST_INCOME
    select
        a.customer_id,
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        -- SAS: lending income (loan interest accrual)
        sum(case when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then a.current_balance * a.interest_rate / 12 else 0 end)
            as lending_income,

        -- SAS: deposit cost (interest paid on deposits)
        sum(case when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then a.current_balance * a.interest_rate / 12 else 0 end)
            as deposit_cost,

        -- SAS: NET_INTEREST_INCOME = LENDING_INCOME - DEPOSIT_COST
        sum(case when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then a.current_balance * a.interest_rate / 12 else 0 end)
        - sum(case when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then a.current_balance * a.interest_rate / 12 else 0 end)
            as net_interest_income,

        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship

    from {{ ref('int_account_metrics') }} a
    group by a.customer_id
),

fee_income as (
    -- SAS Step 2: PROC SQL creating WORK.FEE_INCOME
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
    -- SAS Step 3: PROC SQL creating WORK.ECL
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    group by r.customer_id
),

-- SAS Step 4: DATA step MERGE BY CUSTOMER_ID
pnl as (
    select
        ii.customer_id,
        {{ var('prev_ym') }} as report_month,
        ii.customer_segment,
        ii.region_code,
        ii.branch_id,
        ii.num_accounts,
        ii.total_relationship,
        ii.lending_income,
        ii.deposit_cost,
        ii.net_interest_income,
        coalesce(fi.fee_income, 0) as fee_income,
        coalesce(fi.int_credited, 0) as int_credited,
        coalesce(fi.txn_volume, 0) as txn_volume,

        -- SAS: OPERATING_COST = NUM_ACCOUNTS * 15
        ii.num_accounts * 15 as operating_cost,

        -- SAS: TOTAL_REVENUE = NET_INTEREST_INCOME + FEE_INCOME
        ii.net_interest_income + coalesce(fi.fee_income, 0) as total_revenue,

        coalesce(e.total_ecl, 0) as total_ecl,

        -- SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - ECL
        (ii.net_interest_income + coalesce(fi.fee_income, 0))
            - (ii.num_accounts * 15)
            - coalesce(e.total_ecl, 0) as net_profit,

        -- SAS: ROA (annualized)
        case
            when ii.total_relationship > 0
            then (((ii.net_interest_income + coalesce(fi.fee_income, 0))
                    - (ii.num_accounts * 15)
                    - coalesce(e.total_ecl, 0)) * 12)
                 / ii.total_relationship
            else null
        end as roa,

        -- SAS: PROFIT_TIER IF/THEN assignment
        case
            when (ii.net_interest_income + coalesce(fi.fee_income, 0))
                 - (ii.num_accounts * 15)
                 - coalesce(e.total_ecl, 0) >= 500
                then 'Highly Profitable'
            when (ii.net_interest_income + coalesce(fi.fee_income, 0))
                 - (ii.num_accounts * 15)
                 - coalesce(e.total_ecl, 0) >= 100
                then 'Profitable'
            when (ii.net_interest_income + coalesce(fi.fee_income, 0))
                 - (ii.num_accounts * 15)
                 - coalesce(e.total_ecl, 0) >= 0
                then 'Marginal'
            else 'Unprofitable'
        end as profit_tier,

        current_timestamp() as load_timestamp

    from interest_income ii
    left join fee_income fi
        on ii.customer_id = fi.customer_id
    left join ecl e
        on ii.customer_id = e.customer_id
)

select * from pnl
