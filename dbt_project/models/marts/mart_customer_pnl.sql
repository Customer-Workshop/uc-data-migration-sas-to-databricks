/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL — interest income by customer from STG_BANK.CUST_ACCOUNTS_DAILY
    Step 2: PROC SQL — fee income from CURATED.DAILY_TRANSACTIONS
    Step 3: PROC SQL — expected credit loss from CURATED.RISK_SCORES
    Step 4: DATA step MERGE BY CUSTOMER_ID — P&L assembly, operating cost
            allocation, ROA, and profitability tier assignment

  dbt Equivalent:
    Multi-CTE model joining stg_cust_accounts, stg_daily_transactions, and
    mart_risk_scores.  SAS MERGE BY with IF A → LEFT JOIN on customer_id.
    SAS DATA step IF/THEN (profit tier) → CASE WHEN.

  Quirks reproduced from SAS source (flagged, not corrected):
    Q1  MAX(customer_segment / region_code / branch_id) takes the
        alphabetical maximum, not the value from the "largest account" as
        the SAS comment claims.  Source-faithful.
    Q2  INTEREST_RATE is used as-is in (balance * rate / 12).  If the
        source stores rates in percentage form (e.g. 5.25 for 5.25 %),
        interest figures are 100x the conventional (decimal 0.0525) form.
        The SAS code contains no / 100 divisor — reproduced exactly.
    Q3  SAS SUM() ignores missings — mapped to COALESCE(..., 0).
    Q4  SAS source filters on SNAPSHOT_DATE but the raw Databricks table
        has no snapshot_date column; we treat the current table state as
        the effective month-end snapshot.
    Q5  HELC (Home Equity LOC) is classified as a lending product in Step
        1 but is absent from the deposit list.  Any account type not in
        either list contributes 0 to both buckets — silent pass-through.
*/

with report_dates as (
    select
        to_date('{{ var("prev_ym") }}' || '01', 'yyyyMMdd') as month_start,
        last_day(to_date('{{ var("prev_ym") }}' || '01', 'yyyyMMdd')) as month_end,
        '{{ var("prev_ym") }}' as report_month
),

/* ------------------------------------------------------------------
   Step 1: Interest Income by Customer
   SAS: PROC SQL from STG_BANK.CUST_ACCOUNTS_DAILY grouped by CUSTOMER_ID
   ------------------------------------------------------------------ */
interest_income as (
    select
        a.customer_id,
        /* Q1: alphabetical max, not "largest account" */
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        /* Lending income — MTG, AUTO, PERS, CC, LOC, HELC */
        sum(
            case
                when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as lending_income,

        /* Deposit cost — CHK, SAV, MMA, CD, IRA */
        sum(
            case
                when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as deposit_cost,

        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship

    from {{ ref('stg_cust_accounts') }} a
    group by a.customer_id
),

/* ------------------------------------------------------------------
   Step 2: Fee Income from Transactions
   SAS: PROC SQL from CURATED.DAILY_TRANSACTIONS (has CUSTOMER_ID).
   Raw daily_transactions lacks customer_id; join via stg_cust_accounts.
   ------------------------------------------------------------------ */
fee_income as (
    select
        acct.customer_id,
        sum(
            case
                when t.transaction_type = 'FEE' then abs(t.transaction_amount)
                else 0
            end
        ) as fee_income,
        sum(
            case
                when t.transaction_type = 'INT' then abs(t.transaction_amount)
                else 0
            end
        ) as int_credited,
        count(*) as txn_volume
    from {{ ref('stg_daily_transactions') }} t
    inner join {{ ref('stg_cust_accounts') }} acct
        on t.account_id = acct.account_id
    cross join report_dates rd
    where t.transaction_date between rd.month_start and rd.month_end
    group by acct.customer_id
),

/* ------------------------------------------------------------------
   Step 3: Expected Credit Loss by Customer
   SAS: PROC SQL from CURATED.RISK_SCORES — latest score_date <= month_end
   ------------------------------------------------------------------ */
ecl as (
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    where r.score_date = (
        select max(r2.score_date)
        from {{ ref('mart_risk_scores') }} r2
        cross join report_dates rd
        where r2.score_date <= rd.month_end
    )
    group by r.customer_id
),

/* ------------------------------------------------------------------
   Step 4: Customer P&L Assembly
   SAS: DATA step MERGE BY CUSTOMER_ID with IF A (keep interest_income
   driver); SAS SUM() ignores missings → COALESCE(..., 0)
   ------------------------------------------------------------------ */
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
        /* Operating cost: $15 / account / month */
        ii.num_accounts * 15 as operating_cost,
        /* Total Revenue = SAS sum(NET_INTEREST_INCOME, FEE_INCOME, 0) */
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0) as total_revenue,
        rd.report_month
    from interest_income ii
    left join fee_income fi
        on ii.customer_id = fi.customer_id
    left join ecl
        on ii.customer_id = ecl.customer_id
    cross join report_dates rd
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
    total_revenue - operating_cost - total_ecl as net_profit,
    case
        when total_relationship > 0
            then ((total_revenue - operating_cost - total_ecl) * 12)
                / total_relationship
        else null
    end as roa,
    case
        when total_revenue - operating_cost - total_ecl >= 500
            then 'Highly Profitable'
        when total_revenue - operating_cost - total_ecl >= 100
            then 'Profitable'
        when total_revenue - operating_cost - total_ecl >= 0
            then 'Marginal'
        else 'Unprofitable'
    end as profit_tier,
    report_month
from assembled
