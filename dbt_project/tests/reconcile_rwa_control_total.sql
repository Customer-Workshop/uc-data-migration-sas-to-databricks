/*
  Reconciliation control: RWA control totals tie out to the source.

  Two SUMs must reconcile to the daily snapshot (int_account_metrics):
    1. total_exposure  == sum(current_balance)               (no balance lost/dup)
    2. rwa             == sum(current_balance * risk_weight)  (weighting preserved)

  The expected RWA is recomputed from the raw inputs using the SAS risk-weight
  CASE (source of truth), independently of the mart.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected as (
    select
        sum(a.current_balance) as exposure,
        sum(
            a.current_balance * (
                case
                    when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
                    when a.account_type = 'CD' then 0.00
                    when a.account_type = 'MTG' and l.ltv <= 0.80 then 0.35
                    when a.account_type = 'MTG' and l.ltv > 0.80 then 0.50
                    when a.account_type = 'HELC' then 0.50
                    when a.account_type in ('AUTO', 'PERS') then 0.75
                    when a.account_type = 'CC' then 0.75
                    when a.account_type = 'LOC' then 1.00
                    else 1.00
                end
            )
        ) as rwa
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
),

mart as (
    select
        coalesce(sum(total_exposure), 0) as exposure,
        coalesce(sum(rwa), 0) as rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    e.exposure as expected_exposure,
    m.exposure as mart_exposure,
    e.rwa as expected_rwa,
    m.rwa as mart_rwa
from expected e
cross join mart m
where abs(m.exposure - e.exposure) > 0.01
   or abs(m.rwa - e.rwa) > 0.01
