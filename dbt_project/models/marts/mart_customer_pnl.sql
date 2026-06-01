/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL — interest income by customer from STG_BANK.CUST_ACCOUNTS_DAILY
    Step 2: PROC SQL — fee income from CURATED.DAILY_TRANSACTIONS
    Step 3: PROC SQL — expected credit loss from CURATED.RISK_SCORES
    Step 4: DATA step MERGE — assemble customer P&L with operating cost, ROA, tier

  dbt Equivalent:
    CTEs replace WORK tables; LEFT JOINs replace SAS MERGE BY + IF A.
    CASE expressions reproduce SAS IF/THEN tier assignment.

  Source-faithful quirks (flagged, not fixed):
    Q1 — max(customer_segment/region_code/branch_id) is an arbitrary
          tiebreaker when a customer spans segments/regions/branches.
    Q2 — ORA_DW.COST_OF_FUNDS is listed in the program header but never
          referenced in the SQL; interest_rate on each account is used instead.
    Q3 — Operating cost is hard-coded at $15 per account per month.
    Q4 — INT_CREDITED (Step 2) is computed but never consumed in the P&L.
*/

with interest_income as (
    /* SAS Step 1: PROC SQL from STG_BANK.CUST_ACCOUNTS_DAILY
       where SNAPSHOT_DATE = month_end.
       In dbt, int_account_metrics represents the current snapshot
       (no historical partitions), equivalent to the latest month-end. */
    select
        a.customer_id,
        /* Q1: max() is the SAS tiebreaker — source-faithful, not an endorsement */
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,
        /* Lending income: account types MTG, AUTO, PERS, CC, LOC, HELC */
        sum(
            case
                when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as lending_income,
        /* Deposit cost: account types CHK, SAV, MMA, CD, IRA */
        sum(
            case
                when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as deposit_cost,
        count(distinct a.account_id) as num_accounts,
        sum(a.current_balance) as total_relationship
    from {{ ref('int_account_metrics') }} a
    group by a.customer_id
),

fee_income as (
    /* SAS Step 2: PROC SQL from CURATED.DAILY_TRANSACTIONS
       where TRANSACTION_DATE between month_start and month_end.
       In dbt, stg_daily_transactions lacks customer_id; joined via accounts. */
    select
        acct.customer_id,
        sum(
            case
                when t.transaction_type = 'FEE'
                    then abs(t.transaction_amount)
                else 0
            end
        ) as fee_income,
        /* Q4: INT_CREDITED computed but never consumed in Step 4 */
        sum(
            case
                when t.transaction_type = 'INT'
                    then abs(t.transaction_amount)
                else 0
            end
        ) as int_credited,
        count(*) as txn_volume
    from {{ ref('stg_daily_transactions') }} t
    inner join {{ ref('int_account_metrics') }} acct
        on t.account_id = acct.account_id
    group by acct.customer_id
),

ecl as (
    /* SAS Step 3: PROC SQL from CURATED.RISK_SCORES
       where SCORE_DATE = max(SCORE_DATE) where <= month_end.
       mart_risk_scores is rebuilt each run (score_date = current_date()),
       so it always represents the latest scores. */
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    group by r.customer_id
),

pnl_base as (
    /* SAS Step 4: DATA step MERGE ... BY CUSTOMER_ID; IF A;
       "if a" keeps only customers present in interest_income (with accounts).
       LEFT JOIN to fee_income and ecl reproduces the MERGE semantics. */
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
        /* Q3: hard-coded $15/account/month */
        ii.num_accounts * 15 as operating_cost,
        /* SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
           SAS sum() ignores missing — coalesce reproduces that */
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0) as total_revenue
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
    total_revenue - operating_cost - total_ecl as net_profit,
    /* SAS: ROA = (NET_PROFIT * 12) / TOTAL_RELATIONSHIP when > 0, else . */
    case
        when total_relationship > 0
            then (total_revenue - operating_cost - total_ecl) * 12
                 / total_relationship
        else null
    end as roa,
    /* SAS: Profitability tier — IF/THEN/ELSE chain */
    case
        when total_revenue - operating_cost - total_ecl >= 500
            then 'Highly Profitable'
        when total_revenue - operating_cost - total_ecl >= 100
            then 'Profitable'
        when total_revenue - operating_cost - total_ecl >= 0
            then 'Marginal'
        else 'Unprofitable'
    end as profit_tier,
    '{{ var("prev_ym") }}' as report_month
from pnl_base
