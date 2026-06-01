/*
  Reconciliation test: net_interest_income control total.

  Verifies that the mart's total net_interest_income equals the independently
  calculated (total lending income - total deposit cost) from the account source.
  This ties the mart's aggregated NII back to first principles, catching any
  miscategorisation of account types or arithmetic drift.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with source_calc as (
    select
        sum(
            case
                when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as total_lending_income,
        sum(
            case
                when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as total_deposit_cost
    from {{ ref('stg_cust_accounts') }}
),

mart_calc as (
    select sum(net_interest_income) as total_nii
    from {{ ref('mart_customer_pnl') }}
)

select
    s.total_lending_income - s.total_deposit_cost as expected_nii,
    m.total_nii as actual_nii,
    abs(
        (s.total_lending_income - s.total_deposit_cost) - m.total_nii
    ) as abs_difference
from source_calc s
cross join mart_calc m
where abs(
    (s.total_lending_income - s.total_deposit_cost) - m.total_nii
) > 0.01
