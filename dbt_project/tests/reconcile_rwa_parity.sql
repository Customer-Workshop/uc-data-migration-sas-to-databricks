/*
  Reconciliation test: RWA risk-weight PARITY against the SAS source — value-for-value.

  This is the most critical control for this conversion and the canonical
  source-of-truth trap from the playbook. monthly_regulatory_reporting.sas (Step 1)
  maps each account type to a Basel III risk weight; the mart must reproduce that
  mapping EXACTLY, including:
      LOC  -> 1.00   (NOT 0.75 — "improving" LOC to match CC/PERS overstates
                      capital relief on every line of credit and diverges from source)
      MTG  -> 0.35 when LTV <= 0.80, else 0.50 (and -> 1.00 catch-all when LTV is null)
      else -> 1.00

  It re-derives the expected weight per account from int_account_metrics (+ collateral
  for LTV) using the exact SAS CASE, aggregates to (account_type, risk_weight, count),
  and full-outer-joins to the mart's own (account_type, risk_weight, n_accounts).
  Any account_type whose weight or per-weight count does not match the source — e.g.
  LOC appearing at 0.75 instead of 1.00 — surfaces as a returned row and fails the
  build. An aggregate-only check can pass while an individual branch is wrong; this
  compares each branch.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with derived as (
    select
        a.account_type,
        cast(
            case
                when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
                when a.account_type = 'CD' then 0.00
                when a.account_type = 'MTG'
                    and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) <= 0.80
                    then 0.35
                when a.account_type = 'MTG'
                    and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) > 0.80
                    then 0.50
                when a.account_type = 'HELC' then 0.50
                when a.account_type in ('AUTO', 'PERS') then 0.75
                when a.account_type = 'CC' then 0.75
                when a.account_type = 'LOC' then 1.00
                else 1.00
            end as decimal(5, 2)
        ) as risk_weight
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected as (
    select
        account_type,
        risk_weight,
        count(*) as n
    from derived
    group by account_type, risk_weight
),

actual as (
    select
        account_type,
        cast(risk_weight as decimal(5, 2)) as risk_weight,
        sum(n_accounts) as n
    from {{ ref('mart_regulatory_rwa') }}
    group by account_type, cast(risk_weight as decimal(5, 2))
)

select
    coalesce(e.account_type, a.account_type) as account_type,
    coalesce(e.risk_weight, a.risk_weight) as risk_weight,
    e.n as expected_accounts,
    a.n as actual_accounts
from expected e
full outer join actual a
    on e.account_type = a.account_type and e.risk_weight = a.risk_weight
where coalesce(e.n, 0) <> coalesce(a.n, 0)
