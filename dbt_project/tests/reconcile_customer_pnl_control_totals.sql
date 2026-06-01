/*
  Reconciliation test (CONTROL TOTALS): customer_profitability.sas

  Each control re-derives a SUM straight from the source population and asserts
  the mart ties out to it. A correct grand total is necessary but not sufficient
  (see the parity tests for per-value coverage), but a broken join, dropped
  branch, or wrong scope filter shows up here as a non-zero difference.

  Controls (penny tolerance for floating-point):
    1. total_relationship  = SUM(current_balance) over the in-scope accounts.
    2. num_accounts        = COUNT(*) of in-scope accounts (one per account).
    3. net_interest_income = SUM(lending - deposit), re-derived from the SAS
                             account-type CASE against int_account_metrics.
    4. fee_income          = SUM(abs(amount)) for TRANSACTION_TYPE='FEE' in the
                             report month (the SAS Step 2 scope).
    5. total_ecl           = SUM(expected_loss) over the latest risk scores.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart as (
    select * from {{ ref('mart_customer_pnl') }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
),

txns as (
    select * from {{ ref('mart_daily_transactions') }}
),

risk as (
    select * from {{ ref('mart_risk_scores') }}
),

checks as (
    select 'total_relationship' as control,
           (select sum(total_relationship) from mart) as model_value,
           (select sum(current_balance) from accounts) as source_value
    union all
    select 'num_accounts',
           (select sum(num_accounts) from mart),
           (select count(*) from accounts)
    union all
    select 'net_interest_income',
           (select sum(net_interest_income) from mart),
           (select sum(
               case when account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
                    then current_balance * interest_rate / 12 else 0 end
             - case when account_type in ('CHK','SAV','MMA','CD','IRA')
                    then current_balance * interest_rate / 12 else 0 end
           ) from accounts)
    union all
    -- Scoped to the in-scope customer population: SAS Step 4 MERGE "if a" keeps
    -- only customers that hold an account, so fees for customers with month
    -- transactions but no account are dropped from the P&L by design.
    select 'fee_income',
           (select sum(fee_income) from mart),
           (select sum(case when transaction_type = 'FEE'
                            then abs(transaction_amount) else 0 end)
            from txns
            where date_format(transaction_date, 'yyyyMM') = '{{ var("prev_ym") }}'
              and customer_id in (select customer_id from accounts))
    union all
    select 'total_ecl',
           (select sum(total_ecl) from mart),
           (select sum(expected_loss) from risk
            where score_date = (select max(score_date) from risk))
)

select
    control,
    model_value,
    source_value,
    coalesce(model_value, 0) - coalesce(source_value, 0) as difference
from checks
where abs(coalesce(model_value, 0) - coalesce(source_value, 0)) > 0.01
