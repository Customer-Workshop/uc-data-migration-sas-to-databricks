/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    3x PROC SQL work tables (interest income, fee income, ECL) →
    DATA step MERGE BY CUSTOMER_ID with P&L calculations, ROA,
    and profitability tier assignment.

  dbt Equivalent:
    Three source CTEs replace PROC SQL work tables.
    LEFT JOINs replace SAS MERGE BY (1:1 on customer_id).
    An intermediate CTE replaces SAS "calculated" keyword (alias reuse).
    CASE WHEN replaces SAS IF/THEN tier assignment.
    SAS sum(a, b, 0) → coalesce(a,0) + coalesce(b,0) because SAS sum()
    ignores missings while SQL + propagates NULLs.

  Outputs: REPORTS.CUSTOMER_PNL
  SAS Schedule: Monthly 10th business day via Control-M BANK_MONTHLY_03
*/

with report_params as (
    select
        to_date(concat('{{ var("prev_ym") }}', '01'), 'yyyyMMdd') as month_start,
        last_day(to_date(concat('{{ var("prev_ym") }}', '01'), 'yyyyMMdd')) as month_end,
        '{{ var("prev_ym") }}' as report_month
),

-- SAS Step 1: WORK.INTEREST_INCOME
-- PROC SQL from STG_BANK.CUST_ACCOUNTS_DAILY grouped by CUSTOMER_ID
-- "calculated" keyword replaced by repeating the CASE expressions
interest_income as (
    select
        a.customer_id,
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        -- SAS: LENDING_INCOME
        sum(
            case
                when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as lending_income,

        -- SAS: DEPOSIT_COST
        sum(
            case
                when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as deposit_cost,

        -- SAS: NET_INTEREST_INCOME = calculated LENDING_INCOME - calculated DEPOSIT_COST
        sum(
            case
                when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) - sum(
            case
                when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as net_interest_income,

        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship

    from {{ ref('int_account_metrics') }} a
    cross join report_params rp
    where a.snapshot_date = rp.month_end
    group by a.customer_id
),

-- SAS Step 2: WORK.FEE_INCOME
-- PROC SQL from CURATED.DAILY_TRANSACTIONS for the report month
fee_income as (
    select
        t.customer_id,
        sum(
            case when t.transaction_type = 'FEE' then abs(t.transaction_amount) else 0 end
        ) as fee_income,
        sum(
            case when t.transaction_type = 'INT' then abs(t.transaction_amount) else 0 end
        ) as int_credited,
        count(*) as txn_volume
    from {{ ref('mart_daily_transactions') }} t
    cross join report_params rp
    where t.transaction_date between rp.month_start and rp.month_end
    group by t.customer_id
),

-- SAS Step 3: WORK.ECL
-- PROC SQL from CURATED.RISK_SCORES using latest score_date <= month_end
ecl as (
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    cross join report_params rp
    where r.score_date = (
        select max(r2.score_date)
        from {{ ref('mart_risk_scores') }} r2
        where r2.score_date <= rp.month_end
    )
    group by r.customer_id
),

-- SAS Step 4: DATA step MERGE BY CUSTOMER_ID → LEFT JOINs
-- Intermediate CTE computes net_profit once (avoids repeating the expression
-- in ROA and profit_tier, replacing SAS's ability to reuse variables)
assembled as (
    select
        ii.customer_id,
        ii.customer_segment,
        {{ format_customer_segment('ii.customer_segment') }} as customer_segment_desc,
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
        coalesce(ecl.total_ecl, 0) as total_ecl,

        -- SAS: OPERATING_COST = NUM_ACCOUNTS * 15
        ii.num_accounts * 15 as operating_cost,

        -- SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        -- SAS sum() ignores missing; use coalesce to match
        coalesce(ii.net_interest_income, 0) + coalesce(fi.fee_income, 0) as total_revenue,

        -- SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - coalesce(TOTAL_ECL, 0)
        (coalesce(ii.net_interest_income, 0) + coalesce(fi.fee_income, 0))
          - (ii.num_accounts * 15)
          - coalesce(ecl.total_ecl, 0) as net_profit,

        rp.report_month

    from interest_income ii
    left join fee_income fi
        on ii.customer_id = fi.customer_id
    left join ecl
        on ii.customer_id = ecl.customer_id
    cross join report_params rp
)

select
    customer_id,
    customer_segment,
    customer_segment_desc,
    region_code,
    branch_id,
    num_accounts,
    total_relationship,
    lending_income,
    deposit_cost,
    net_interest_income,
    fee_income,
    int_credited,
    txn_volume,
    total_ecl,
    operating_cost,
    total_revenue,
    net_profit,

    -- SAS: ROA (annualized) with null for zero-relationship customers
    case
        when total_relationship > 0
        then (net_profit * 12) / total_relationship
        else null
    end as roa,

    -- SAS: Profitability tier (IF/THEN → CASE WHEN)
    case
        when net_profit >= 500 then 'Highly Profitable'
        when net_profit >= 100 then 'Profitable'
        when net_profit >= 0   then 'Marginal'
        else 'Unprofitable'
    end as profit_tier,

    report_month

from assembled
