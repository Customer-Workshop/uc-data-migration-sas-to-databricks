/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 2 EAD preserves
  drawn balances for non-revolving accounts and applies the documented 50%
  conversion factor to CC/LOC/HELC undrawn limits. It independently computes
  both control totals from int_account_metrics.
*/
with source_totals as (
    select
        sum(current_balance) as current_balance,
        sum(
            case
                when account_type in ('CC', 'LOC', 'HELC')
                    then current_balance + 0.50 * (credit_limit - current_balance)
                else current_balance
            end
        ) as ead
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
      and snapshot_date = current_date()
),

mart_totals as (
    select
        sum(current_balance) as current_balance,
        sum(ead) as ead
    from {{ ref('mart_risk_scores') }}
)

select
    s.current_balance as expected_current_balance,
    m.current_balance as actual_current_balance,
    s.ead as expected_ead,
    m.ead as actual_ead
from source_totals s
cross join mart_totals m
where abs(s.current_balance - m.current_balance) > 0.01
   or abs(s.ead - m.ead) > 0.01
