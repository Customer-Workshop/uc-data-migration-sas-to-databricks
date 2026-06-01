/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL — interest income by customer from
            STG_BANK.CUST_ACCOUNTS_DAILY (lending income - deposit cost).
    Step 2: PROC SQL — fee income from CURATED.DAILY_TRANSACTIONS, scoped
            to the report month.
    Step 3: PROC SQL — expected credit loss from CURATED.RISK_SCORES
            (latest score on/before period end).
    Step 4: DATA step MERGE BY CUSTOMER_ID (if a) — assembles the P&L with
            operating-cost allocation, total revenue, net profit, ROA, tier.
  (Step 5 — PROC MEANS segment/branch rollups and the %export_xlsx side
   effect — are out of scope for this customer-grain mart.)

  dbt Equivalent:
    - SAS LIBNAME reads map to ref()s: STG_BANK.CUST_ACCOUNTS_DAILY ->
      int_account_metrics, CURATED.DAILY_TRANSACTIONS ->
      mart_daily_transactions, CURATED.RISK_SCORES -> mart_risk_scores.
    - Multi-dataset MERGE BY (if a) -> LEFT JOIN anchored on the account
      population (Step 1), so customers without txns/risk scores are kept.
    - SAS DATA-step IF/THEN tiering -> CASE; SAS calculated columns -> SQL.
    - SAS macro var &report_month (autoexec PREV_YM, yymmn6) -> var('prev_ym').

  SOURCE-FAITHFUL QUIRKS (reproduced exactly, NOT corrected — flagged for a
  separate business decision; see PR):
    1. Interest is `current_balance * interest_rate / 12` with NO division by
       100. interest_rate is stored as a percentage (e.g. 5.25), so the SAS
       monthly interest is overstated ~100x. Reproduced verbatim from the
       source PROC SQL.
    2. customer_segment / region_code / branch_id use MAX() per customer. The
       SAS comment says "from largest account", but the code takes the
       lexical/numeric MAX, NOT the attribute of the largest-balance account.
       Reproduced as MAX() to match the source value-for-value.
    3. ORA_DW.COST_OF_FUNDS is declared as an input in the SAS header but is
       never read in the program body — there is nothing to migrate.

  DOCUMENTED SEMANTIC ADAPTATION (not a silent change):
    - SAS Step 3 selects the latest RISK_SCORES row with SCORE_DATE on/before
      month_end against a *historical* scores table. The upstream converted
      mart_risk_scores carries a single current snapshot (score_date = run
      date), so "latest score as of the period" reduces to max(score_date).
      A literal `<= month_end` bound would discard the only snapshot and zero
      out all credit losses, which is not the source's intent.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

transactions as (
    select * from {{ ref('mart_daily_transactions') }}
),

risk_scores as (
    select * from {{ ref('mart_risk_scores') }}
),

-- SAS Step 1: Interest income by customer (anchor population — "if a")
interest_income as (
    select
        customer_id,
        -- Quirk 2: SAS uses MAX() despite the "largest account" comment.
        max(customer_segment) as customer_segment,
        max(region_code) as region_code,
        max(branch_id) as branch_id,
        -- Quirk 1: balance * rate / 12, no /100 (rate is a percentage).
        sum(case
            when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then current_balance * interest_rate / 12
            else 0
        end) as lending_income,
        sum(case
            when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then current_balance * interest_rate / 12
            else 0
        end) as deposit_cost,
        count(distinct account_id) as num_accounts,
        sum(current_balance) as total_relationship
    from accounts
    group by customer_id
),

-- SAS Step 2: Fee income from transactions, scoped to the report month
-- SAS: WHERE TRANSACTION_DATE between "&month_start"d and "&month_end"d
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
    from transactions
    where date_format(transaction_date, 'yyyyMM') = '{{ var("prev_ym") }}'
    group by customer_id
),

-- SAS Step 3: Expected credit loss by customer (latest available scores)
ecl as (
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from risk_scores r
    where r.score_date = (select max(rr.score_date) from risk_scores rr)
    group by r.customer_id
),

-- SAS Step 4: MERGE WORK.INTEREST_INCOME (in=a) WORK.FEE_INCOME WORK.ECL;
--             by CUSTOMER_ID; if a;  -> LEFT JOIN anchored on Step 1
pnl as (
    select
        ii.customer_id,
        ii.customer_segment,
        ii.region_code,
        ii.branch_id,
        ii.num_accounts,
        ii.total_relationship,
        ii.lending_income,
        ii.deposit_cost,

        -- SAS: NET_INTEREST_INCOME = LENDING_INCOME - DEPOSIT_COST
        ii.lending_income - ii.deposit_cost as net_interest_income,

        -- SAS: FEE_INCOME / INT_CREDITED / TXN_VOLUME (missing -> 0 on no match)
        coalesce(fi.fee_income, 0) as fee_income,
        coalesce(fi.int_credited, 0) as int_credited,
        coalesce(fi.txn_volume, 0) as txn_volume,

        -- SAS: OPERATING_COST = NUM_ACCOUNTS * 15 ($15/account/month)
        ii.num_accounts * 15 as operating_cost,

        -- SAS: TOTAL_ECL (coalesce(...,0) applied where used below)
        coalesce(e.total_ecl, 0) as total_ecl,

        -- SAS: TOTAL_REVENUE = sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0) as total_revenue,

        -- SAS: NET_PROFIT = TOTAL_REVENUE - OPERATING_COST - coalesce(TOTAL_ECL,0)
        (ii.lending_income - ii.deposit_cost)
            + coalesce(fi.fee_income, 0)
            - (ii.num_accounts * 15)
            - coalesce(e.total_ecl, 0) as net_profit,

        -- SAS: ROA = (NET_PROFIT * 12) / TOTAL_RELATIONSHIP if > 0 else .
        case
            when ii.total_relationship > 0
            then (
                (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) * 12 / ii.total_relationship
            else null
        end as roa,

        -- SAS: PROFIT_TIER IF/THEN ladder (>=500 / >=100 / >=0 / else)
        case
            when (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 500 then 'Highly Profitable'
            when (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 100 then 'Profitable'
            when (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0) >= 0 then 'Marginal'
            else 'Unprofitable'
        end as profit_tier,

        -- SAS: REPORT_MONTH = "&report_month" (autoexec PREV_YM, yymmn6)
        '{{ var("prev_ym") }}' as report_month,
        current_timestamp() as load_timestamp

    from interest_income ii
    left join fee_income fi on ii.customer_id = fi.customer_id
    left join ecl e on ii.customer_id = e.customer_id
)

select * from pnl
