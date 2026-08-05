/*
  Reconciliation test: RWA exposure and weighted total tie to an
  independently recomputed account-level result using the SAS mapping.

  A 0.01 absolute tolerance accommodates double arithmetic only; it does not
  relax the source-parity control.
*/
with account_ltv as (
    select
        a.current_balance,
        a.account_type,
        c.collateral_value,
        case
            when c.collateral_value > 0
            then a.current_balance / c.collateral_value
            else null
        end as ltv
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected as (
    select
        sum(current_balance) as total_exposure,
        sum(
            current_balance
            * case
                when account_type in ('CHK', 'SAV', 'MMA') then 0.00
                when account_type = 'CD' then 0.00
                when account_type = 'MTG' and (ltv <= 0.80 or ltv is null) then 0.35
                when account_type = 'MTG' and ltv > 0.80 then 0.50
                when account_type = 'HELC' then 0.50
                when account_type in ('AUTO', 'PERS') then 0.75
                when account_type = 'CC' then 0.75
                when account_type = 'LOC' then 1.00
                else 1.00
            end
        ) as rwa
    from account_ltv
),

actual as (
    select
        sum(total_exposure) as total_exposure,
        sum(rwa) as rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    e.total_exposure as expected_total_exposure,
    a.total_exposure as model_total_exposure,
    e.rwa as expected_rwa,
    a.rwa as model_rwa
from expected e
cross join actual a
where abs(coalesce(e.total_exposure, 0) - coalesce(a.total_exposure, 0)) > 0.01
   or abs(coalesce(e.rwa, 0) - coalesce(a.rwa, 0)) > 0.01
