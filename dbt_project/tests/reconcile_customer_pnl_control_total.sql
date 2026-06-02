/*
  Reconciliation test: customer P&L control totals tie out to source.

  Every dollar in mart_customer_pnl must be reconstructable from the source the SAS
  program read. We recompute each P&L line independently from the source models and
  compare to the mart's aggregate:

    net_interest_income : sum(lending bal*rate/12) - sum(deposit bal*rate/12)
                          over int_account_metrics (the daily account snapshot)
    fee_income          : sum(abs(amount)) for FEE txns in mart_daily_transactions,
                          restricted to in-scope customers
    operating_cost      : $15 * number of in-scope accounts
    total_ecl           : sum(expected_loss) at the latest score date, in-scope
    net_profit          : nii + fee - operating_cost - ecl

  A tiny floating-point tolerance is allowed; any real divergence (a dropped CASE
  branch, a fanned-out join, a wrong constant) produces a large diff and fails.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with in_scope as (
    select distinct customer_id from {{ ref('int_account_metrics') }}
),

latest_score as (
    select max(score_date) as max_score_date
    from {{ ref('mart_risk_scores') }}
    where score_date <= current_date()
),

expected as (
    select
        (
            select
                sum(case when {{ classify_interest_bucket('account_type') }} = 'LENDING'
                    then current_balance * interest_rate / 12 else 0 end)
                - sum(case when {{ classify_interest_bucket('account_type') }} = 'DEPOSIT'
                    then current_balance * interest_rate / 12 else 0 end)
            from {{ ref('int_account_metrics') }}
        ) as nii,
        (
            select sum(case when t.transaction_type = 'FEE'
                then abs(t.transaction_amount) else 0 end)
            from {{ ref('mart_daily_transactions') }} t
            inner join in_scope c on t.customer_id = c.customer_id
        ) as fee,
        (
            select 15 * count(*) from {{ ref('int_account_metrics') }}
        ) as operating_cost,
        (
            select sum(r.expected_loss)
            from {{ ref('mart_risk_scores') }} r
            inner join in_scope c on r.customer_id = c.customer_id
            cross join latest_score l
            where r.score_date = l.max_score_date
        ) as ecl
),

expected_final as (
    select
        nii,
        fee,
        operating_cost,
        ecl,
        nii + coalesce(fee, 0) - operating_cost - coalesce(ecl, 0) as net_profit
    from expected
),

actual as (
    select
        sum(net_interest_income) as nii,
        sum(coalesce(fee_income, 0)) as fee,
        sum(operating_cost) as operating_cost,
        sum(coalesce(total_ecl, 0)) as ecl,
        sum(net_profit) as net_profit
    from {{ ref('mart_customer_pnl') }}
),

comparison as (
    select
        'net_interest_income' as metric,
        e.nii as expected,
        a.nii as actual
    from expected_final e
    cross join actual a

    union all

    select
        'fee_income' as metric,
        e.fee as expected,
        a.fee as actual
    from expected_final e
    cross join actual a

    union all

    select
        'operating_cost' as metric,
        e.operating_cost as expected,
        a.operating_cost as actual
    from expected_final e
    cross join actual a

    union all

    select
        'total_ecl' as metric,
        e.ecl as expected,
        a.ecl as actual
    from expected_final e
    cross join actual a

    union all

    select
        'net_profit' as metric,
        e.net_profit as expected,
        a.net_profit as actual
    from expected_final e
    cross join actual a
)

select
    metric,
    expected,
    actual,
    actual - expected as difference
from comparison
where abs(coalesce(actual, 0) - coalesce(expected, 0)) > 0.01
