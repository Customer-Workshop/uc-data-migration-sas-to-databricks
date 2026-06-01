/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1 interest income (lending income - deposit cost), Step 2 fee income
    from transactions, Step 3 expected credit loss from risk scores, and a
    Step 4 DATA step MERGE BY CUSTOMER_ID assembling the P&L with an IF/THEN
    profitability tier.

  dbt Equivalent:
    The three SAS PROC SQL extracts become CTEs; the MERGE BY becomes LEFT JOINs;
    the IF/THEN tier assignment becomes a CASE expression.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- SAS Step 1: interest income / deposit cost per customer
interest_income as (
    select
        customer_id,
        count(distinct account_id) as num_accounts,
        sum(current_balance) as total_relationship,
        sum(
            case when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                then current_balance * interest_rate / 100 / 12 else 0 end
        ) as lending_income,
        sum(
            case when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                then current_balance * interest_rate / 100 / 12 else 0 end
        ) as deposit_cost
    from accounts
    group by customer_id
),

-- SAS Step 2: fee income from transactions
fee_income as (
    select
        customer_id,
        sum(case when transaction_type = 'FEE' then abs(transaction_amount) else 0 end) as fee_income
    from {{ ref('mart_daily_transactions') }}
    group by customer_id
),

-- SAS Step 3: expected credit loss (risk scores joined back to customer)
ecl as (
    select
        a.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    inner join accounts a on r.account_id = a.account_id
    group by a.customer_id
),

assembled as (
    select
        ii.customer_id,
        date_format(current_date(), 'yyyyMM') as report_month,
        ii.num_accounts,
        ii.total_relationship,
        ii.lending_income - ii.deposit_cost as net_interest_income,
        coalesce(fi.fee_income, 0) as fee_income,
        ii.num_accounts * 15 as operating_cost,
        coalesce(e.total_ecl, 0) as total_ecl
    from interest_income ii
    left join fee_income fi on ii.customer_id = fi.customer_id
    left join ecl e on ii.customer_id = e.customer_id
)

select
    customer_id,
    report_month,
    num_accounts,
    total_relationship,
    net_interest_income,
    fee_income,
    operating_cost,
    total_ecl,
    net_interest_income + fee_income as total_revenue,
    net_interest_income + fee_income - operating_cost - total_ecl as net_profit,
    -- SAS Step 4: IF/THEN profitability tier
    case
        when net_interest_income + fee_income - operating_cost - total_ecl >= 500
            then 'Highly Profitable'
        when net_interest_income + fee_income - operating_cost - total_ecl >= 50
            then 'Profitable'
        when net_interest_income + fee_income - operating_cost - total_ecl >= 0
            then 'Marginal'
        else 'Unprofitable'
    end as profit_tier
from assembled
