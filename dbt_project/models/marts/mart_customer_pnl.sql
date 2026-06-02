/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 4: REPORTS.CUSTOMER_PNL)

  SAS Original:
    DATA step MERGE BY CUSTOMER_ID of three WORK tables (INTEREST_INCOME,
    FEE_INCOME, ECL) with `if a;` subsetting, then derives operating cost,
    total revenue, net profit, ROA, and a profitability tier.

  dbt Equivalent:
    SAS multi-dataset MERGE BY -> multi-ref LEFT JOIN keyed on customer_id.
    `if a;` (keep only rows present in INTEREST_INCOME) -> the interest-income
    model is the driving (left) table, so customers without transactions or risk
    scores are retained with NULL fee/ECL. DATA step derivations -> SQL
    expressions; IF/THEN tier assignment -> CASE.
*/

with interest_income as (
    select * from {{ ref('int_customer_interest_income') }}
),

fee_income as (
    select * from {{ ref('int_customer_fee_income') }}
),

ecl as (
    select * from {{ ref('int_customer_ecl') }}
),

-- SAS: merge INTEREST_INCOME(in=a) FEE_INCOME(in=b) ECL(in=c); by CUSTOMER_ID; if a;
assembled as (
    select
        ii.customer_id,
        ii.customer_segment,
        ii.region_code,
        ii.branch_id,
        ii.lending_income,
        ii.deposit_cost,
        -- SAS: NET_INTEREST_INCOME = LENDING_INCOME - DEPOSIT_COST
        ii.lending_income - ii.deposit_cost as net_interest_income,
        ii.num_accounts,
        ii.total_relationship,
        fi.fee_income,
        fi.int_credited,
        fi.txn_volume,
        e.total_ecl
    from interest_income ii
    left join fee_income fi
        on ii.customer_id = fi.customer_id
    left join ecl e
        on ii.customer_id = e.customer_id
),

derived as (
    select
        *,

        -- SAS: OPERATING_COST = NUM_ACCOUNTS * 15  (simplified $15/account/month)
        num_accounts * 15 as operating_cost,

        -- SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        -- SAS sum() skips missing values, so FEE_INCOME null -> treated as 0.
        net_interest_income + coalesce(fee_income, 0) as total_revenue
    from assembled
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

    -- SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - coalesce(TOTAL_ECL, 0)
    total_revenue - operating_cost - coalesce(total_ecl, 0) as net_profit,

    -- SAS: if TOTAL_RELATIONSHIP > 0 then ROA = (NET_PROFIT * 12) / TOTAL_RELATIONSHIP; else .
    case
        when total_relationship > 0
            then ((total_revenue - operating_cost - coalesce(total_ecl, 0)) * 12)
                / total_relationship
        else null
    end as roa,

    -- SAS: PROFIT_TIER IF/THEN/ELSE ladder on NET_PROFIT
    case
        when (total_revenue - operating_cost - coalesce(total_ecl, 0)) >= 500
            then 'Highly Profitable'
        when (total_revenue - operating_cost - coalesce(total_ecl, 0)) >= 100
            then 'Profitable'
        when (total_revenue - operating_cost - coalesce(total_ecl, 0)) >= 0
            then 'Marginal'
        else 'Unprofitable'
    end as profit_tier,

    -- SAS: REPORT_MONTH = "&report_month"  (batch passes &PREV_YM)
    '{{ var("prev_ym") }}' as report_month

from derived
