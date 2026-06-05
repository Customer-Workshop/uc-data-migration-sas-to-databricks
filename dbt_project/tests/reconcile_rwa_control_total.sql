/*
  Reconciliation test: RWA control total.

  Total RWA in mart_regulatory_rwa (SUM of rwa column) must tie out to
  SUM(current_balance * risk_weight) computed independently from the raw
  source data (int_account_metrics joined with collateral). This proves
  the aggregation in the mart did not introduce rounding or logic drift.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with mart_total as (
    select coalesce(sum(rwa), 0) as total_rwa from {{ ref('mart_regulatory_rwa') }}
),

independent_total as (
    select
        coalesce(sum(
            a.current_balance * case
                when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
                when a.account_type = 'CD' then 0.00
                when a.account_type = 'MTG'
                     and c.collateral_value > 0
                     and (a.current_balance / c.collateral_value) <= 0.80
                    then 0.35
                when a.account_type = 'MTG' then 0.50
                when a.account_type = 'HELC' then 0.50
                when a.account_type in ('AUTO', 'PERS') then 0.75
                when a.account_type = 'CC' then 0.75
                when a.account_type = 'LOC' then 1.00
                else 1.00
            end
        ), 0) as total_rwa
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
)

select
    m.total_rwa as mart_rwa,
    i.total_rwa as independent_rwa,
    abs(m.total_rwa - i.total_rwa) as difference
from mart_total m
cross join independent_total i
where abs(m.total_rwa - i.total_rwa) > 0.01
