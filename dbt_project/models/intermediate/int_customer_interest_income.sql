/*
  int_customer_interest_income.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 1: WORK.INTEREST_INCOME)

  SAS Original:
    PROC SQL ... GROUP BY a.CUSTOMER_ID over STG_BANK.CUST_ACCOUNTS_DAILY for the
    month-end snapshot. Computes lending income, deposit cost, net interest
    margin, account count, and total relationship balance per customer.

  dbt Equivalent:
    PROC SQL GROUP BY -> dbt model with GROUP BY over int_account_metrics (the
    daily account snapshot, mirroring how mart_risk_scores reads it). The two SAS
    account-type IN-lists are encoded once in the classify_interest_bucket macro.

  SAS: STG_BANK.CUST_ACCOUNTS_DAILY where SNAPSHOT_DATE = "&month_end"d
    int_account_metrics is a single daily snapshot (snapshot_date = current_date),
    so no extra date filter is required — same convention as mart_risk_scores.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
)

select
    customer_id,

    -- SAS: max(CUSTOMER_SEGMENT/REGION_CODE/BRANCH_ID) per customer.
    -- NOTE (source-faithful): the SAS comment says "from largest account" but the
    -- code uses MAX(), i.e. the alphabetically-greatest value, NOT the value from
    -- the largest-balance account. Reproduced exactly; not an endorsement.
    max(customer_segment) as customer_segment,
    max(region_code) as region_code,
    max(branch_id) as branch_id,

    -- SAS: LENDING_INCOME = sum(case when ACCOUNT_TYPE in (lending) then bal*rate/12 else 0)
    sum(
        case
            when {{ classify_interest_bucket('account_type') }} = 'LENDING'
                then current_balance * interest_rate / 12
            else 0
        end
    ) as lending_income,

    -- SAS: DEPOSIT_COST = sum(case when ACCOUNT_TYPE in (deposit) then bal*rate/12 else 0)
    sum(
        case
            when {{ classify_interest_bucket('account_type') }} = 'DEPOSIT'
                then current_balance * interest_rate / 12
            else 0
        end
    ) as deposit_cost,

    -- SAS: NUM_ACCOUNTS = count(distinct ACCOUNT_ID)
    count(distinct account_id) as num_accounts,

    -- SAS: TOTAL_RELATIONSHIP = sum(CURRENT_BALANCE).
    -- NOTE (source-faithful): includes balances of account types that fall in
    -- neither IN-list (NEITHER) even though they contribute 0 interest. The 11
    -- seeded/known types are all covered, so NEITHER is empty in practice.
    sum(current_balance) as total_relationship

from accounts
group by customer_id
