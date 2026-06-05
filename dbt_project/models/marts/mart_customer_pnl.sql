/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL interest income by customer (lending income,
            deposit cost, net interest margin)
    Step 2: PROC SQL fee income from CURATED.DAILY_TRANSACTIONS
    Step 3: PROC SQL expected credit loss from CURATED.RISK_SCORES
    Step 4: DATA step MERGE BY CUSTOMER_ID + P&L assembly
            (operating cost, total revenue, net profit, ROA, tier)

  dbt Equivalent:
    CTEs replace WORK tables.
    LEFT JOINs replace SAS MERGE BY.
    CASE expressions replace IF/THEN tier assignment.
*/

with interest_income as (
    -- SAS Step 1: Interest income by customer
    select
        a.customer_id,
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        -- SAS: Lending income (credit products)
        sum(case
            when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                then a.current_balance * a.interest_rate / 12
            else 0
        end) as lending_income,

        -- SAS: Deposit cost (deposit products)
        sum(case
            when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                then a.current_balance * a.interest_rate / 12
            else 0
        end) as deposit_cost,

        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship

    from {{ ref('int_account_metrics') }} a
    group by a.customer_id
),

fee_income as (
    -- SAS Step 2: Fee income from transactions
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
    -- SAS Step 3: Expected credit loss by customer
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    group by r.customer_id
),

-- SAS Step 4: MERGE BY + P&L assembly — base metrics
pnl_base as (
    select
        i.customer_id,
        '{{ var("prev_ym") }}' as report_month,
        i.customer_segment,
        i.region_code,
        i.branch_id,
        i.lending_income,
        i.deposit_cost,
        i.lending_income - i.deposit_cost as net_interest_income,
        i.num_accounts,
        i.total_relationship,
        coalesce(f.fee_income, 0) as fee_income,
        coalesce(f.int_credited, 0) as int_credited,
        coalesce(f.txn_volume, 0) as txn_volume,
        i.num_accounts * 15 as operating_cost,
        (i.lending_income - i.deposit_cost)
            + coalesce(f.fee_income, 0) as total_revenue,
        coalesce(e.total_ecl, 0) as total_ecl
    from interest_income i
    left join fee_income f
        on i.customer_id = f.customer_id
    left join ecl e
        on i.customer_id = e.customer_id
),

-- Derived profit metrics (avoids repeating net_profit expression)
assembled as (
    select
        *,
        total_revenue - operating_cost - total_ecl as net_profit,

        -- SAS: ROA = (NET_PROFIT * 12) / TOTAL_RELATIONSHIP
        case
            when total_relationship > 0
                then (total_revenue - operating_cost - total_ecl)
                     * 12 / total_relationship
            else null
        end as roa,

        -- SAS: Profitability tier
        case
            when total_revenue - operating_cost - total_ecl >= 500
                then 'Highly Profitable'
            when total_revenue - operating_cost - total_ecl >= 100
                then 'Profitable'
            when total_revenue - operating_cost - total_ecl >= 0
                then 'Marginal'
            else 'Unprofitable'
        end as profit_tier

    from pnl_base
)

select * from assembled
