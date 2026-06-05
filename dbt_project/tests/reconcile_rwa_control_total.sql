/*
  Reconciliation: RWA control total — sum(rwa) in the mart must equal the
  independently recomputed sum(balance * risk_weight) from the source tables.

  This catches any aggregation or risk-weight application error: if a single
  CASE branch assigns the wrong weight, or the GROUP BY silently drops rows,
  the total will diverge.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart_total as (
    select coalesce(sum(rwa), 0) as total_rwa
    from {{ ref('mart_regulatory_rwa') }}
),

recomputed as (
    select
        coalesce(sum(
            a.current_balance * case
                when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
                when a.account_type = 'CD'                    then 0.00
                when a.account_type = 'MTG'
                     and case
                             when c.collateral_value > 0
                             then a.current_balance / c.collateral_value
                             else null
                         end <= 0.80                          then 0.35
                when a.account_type = 'MTG'                   then 0.50
                when a.account_type = 'HELC'                  then 0.50
                when a.account_type in ('AUTO', 'PERS')       then 0.75
                when a.account_type = 'CC'                    then 0.75
                when a.account_type = 'LOC'                   then 1.00
                else 1.00
            end
        ), 0) as total_rwa
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
)

select
    r.total_rwa as expected_rwa,
    m.total_rwa as model_rwa,
    abs(m.total_rwa - r.total_rwa) as abs_difference
from recomputed r
cross join mart_total m
where abs(m.total_rwa - r.total_rwa) > 0.01
