/*
  Reconciliation control: RWA control totals tie out to source balances.

  Two totals must agree with the source:
    1. TOTAL_EXPOSURE — sum(CURRENT_BALANCE) over the in-scope month-end
       population. The SAS report aggregates the same balances it reads, so the
       mart total must equal the raw total to the cent.
    2. RWA — must equal sum(exposure x risk weight) recomputed from the mart's
       own group grain, proving the weighted aggregation was not corrupted by
       the join or the GROUP BY.

  Tolerance is one cent, for floating-point summation order only.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_total as (
    select sum(a.current_balance) as total_balance
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
),

mart_total as (
    select
        sum(total_exposure) as total_exposure,
        sum(rwa) as rwa,
        sum(total_exposure * risk_weight) as recomputed_rwa
    from {{ ref('mart_regulatory_rwa') }}
),

checks as (
    select
        'total_exposure' as control,
        s.total_balance as expected_value,
        m.total_exposure as actual_value
    from source_total s
    cross join mart_total m

    union all

    select
        'rwa' as control,
        m.recomputed_rwa as expected_value,
        m.rwa as actual_value
    from mart_total m
)

select
    control,
    expected_value,
    actual_value,
    actual_value - expected_value as difference
from checks
where abs(actual_value - expected_value) > 0.01
