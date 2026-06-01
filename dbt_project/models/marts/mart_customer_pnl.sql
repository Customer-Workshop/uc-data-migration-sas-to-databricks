/*
  mart_customer_pnl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Steps 1-4)

  SAS Original (REPORTS.CUSTOMER_PNL):
    Step 1 — PROC SQL: Interest income by customer from STG_BANK.CUST_ACCOUNTS_DAILY
             at month-end snapshot. Lending income vs deposit cost, net interest income.
    Step 2 — PROC SQL: Fee income from CURATED.DAILY_TRANSACTIONS for the month.
    Step 3 — PROC SQL: Expected credit loss from CURATED.RISK_SCORES (latest score date).
    Step 4 — DATA step MERGE BY CUSTOMER_ID: P&L assembly with operating cost,
             total revenue, net profit, ROA, profit tier.

  dbt Equivalent:
    Four CTEs mirroring the SAS steps, joined via LEFT JOIN (reproducing
    the SAS MERGE BY with `if a;` — only customers with accounts are kept).
    Date logic adapted: stg_cust_accounts is a current-state view (no snapshot
    date filter), so all active in-scope accounts are included. Fee income
    filters to the reporting month. ECL uses the latest score_date from
    mart_risk_scores.

  Conversion notes:
    - The $15/account/month operating cost is a hard-coded simplification
      from the SAS source. Reproduced exactly per the conversion contract.
      [FLAG] Consider replacing with an activity-based cost model.
    - SAS sum(NET_INTEREST_INCOME, FEE_INCOME, 0) treats missing as zero;
      reproduced via coalesce().
    - SAS `if a;` on MERGE = LEFT JOIN from interest_income (the driver).
*/

{% set report_month = var('prev_ym') %}

with interest_income as (
    /*
      SAS Step 1: Interest Income by Customer
      Source: STG_BANK.CUST_ACCOUNTS_DAILY (snapshot at month end)
      dbt:   ref('stg_cust_accounts') — current-state view, no snapshot filter
    */
    select
        a.customer_id,
        max(a.customer_segment) as customer_segment,
        max(a.region_code) as region_code,
        max(a.branch_id) as branch_id,

        -- Lending income: sum(balance * rate / 12) for lending account types
        sum(
            case
                when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then a.current_balance * a.interest_rate / 12
                else 0
            end
        ) as lending_income,

        -- Deposit cost: sum(balance * rate / 12) for deposit account types
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

fee_income as (
    /*
      SAS Step 2: Fee Income from Transactions
      Source: CURATED.DAILY_TRANSACTIONS filtered to reporting month
      dbt:   ref('stg_daily_transactions') joined to stg_cust_accounts for
             customer_id (raw txns have account_id only; the SAS CURATED table
             was pre-enriched with customer_id). Uses the staging model to
             mirror the SAS CURATED source (post-validation, filtered).
    */
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
    where
        t.transaction_date >= to_date('{{ report_month }}' || '01', 'yyyyMMdd')
        and t.transaction_date <= last_day(
            to_date('{{ report_month }}' || '01', 'yyyyMMdd')
        )
    group by acct.customer_id
),

ecl as (
    /*
      SAS Step 3: Expected Credit Loss by Customer
      Source: CURATED.RISK_SCORES — latest score_date <= month end
      dbt:   ref('mart_risk_scores') — score_date = current_date()
      Adapted: mart_risk_scores always stamps score_date = current_date(),
      so the SAS filter (score_date <= month_end) would exclude all rows
      when report_month = prev_ym. Use max(score_date) unconditionally to
      get the latest available scores (matches the SAS intent).
    */
    select
        r.customer_id,
        sum(r.expected_loss) as total_ecl
    from {{ ref('mart_risk_scores') }} r
    where r.score_date = (
        select max(score_date)
        from {{ ref('mart_risk_scores') }}
    )
    group by r.customer_id
),

/*
  SAS Step 4: Customer P&L Assembly
  SAS: DATA step MERGE BY CUSTOMER_ID with `if a;`
  dbt: LEFT JOIN from interest_income (the driver CTE)
*/
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

        coalesce(e.total_ecl, 0) as total_ecl,

        -- Operating cost: $15/account/month (hard-coded SAS simplification)
        -- [FLAG] This is a source-faithful reproduction of the SAS hard-coded
        -- cost allocation. Consider replacing with activity-based costing.
        ii.num_accounts * 15 as operating_cost,

        -- Total revenue: SAS sum(NET_INTEREST_INCOME, FEE_INCOME, 0)
        (ii.lending_income - ii.deposit_cost)
        + coalesce(fi.fee_income, 0) as total_revenue,

        -- Net profit
        (ii.lending_income - ii.deposit_cost)
        + coalesce(fi.fee_income, 0)
        - (ii.num_accounts * 15)
        - coalesce(e.total_ecl, 0) as net_profit,

        -- ROA (annualized): (net_profit * 12) / total_relationship
        case
            when ii.total_relationship > 0
                then (
                    (
                        (ii.lending_income - ii.deposit_cost)
                        + coalesce(fi.fee_income, 0)
                        - (ii.num_accounts * 15)
                        - coalesce(e.total_ecl, 0)
                    ) * 12
                ) / ii.total_relationship
            else null
        end as roa,

        -- Profit tier: SAS IF/THEN thresholds (500/100/0)
        case
            when (
                (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 500 then 'Highly Profitable'
            when (
                (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 100 then 'Profitable'
            when (
                (ii.lending_income - ii.deposit_cost)
                + coalesce(fi.fee_income, 0)
                - (ii.num_accounts * 15)
                - coalesce(e.total_ecl, 0)
            ) >= 0 then 'Marginal'
            else 'Unprofitable'
        end as profit_tier,

        '{{ report_month }}' as report_month

    from interest_income ii
    left join fee_income fi on ii.customer_id = fi.customer_id
    left join ecl e on ii.customer_id = e.customer_id
)

select * from assembled
