/*
  Reconciliation test: RWA risk weight parity (per-value, not aggregate).

  Every distinct (account_type, risk_weight) pair in the mart must match the
  SAS source CASE mapping value-for-value:
    CHK/SAV/MMA = 0.00, CD = 0.00, MTG = 0.35 (LTV<=0.80) or 0.50 (LTV>0.80),
    HELC = 0.50, AUTO/PERS = 0.75, CC = 0.75, LOC = 1.00, else = 1.00.

  This is the critical parity check — a mapping can produce a correct total
  while an individual branch is wrong (see the LOC divergence example in the
  playbook).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with mart_weights as (
    select distinct
        account_type,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

select
    account_type,
    risk_weight as actual_weight
from mart_weights
where
    case
        when account_type in ('CHK', 'SAV', 'MMA')
            then abs(risk_weight - 0.00) > 0.001
        when account_type = 'CD'
            then abs(risk_weight - 0.00) > 0.001
        when account_type = 'MTG'
            then risk_weight not in (0.35, 0.50)
        when account_type = 'HELC'
            then abs(risk_weight - 0.50) > 0.001
        when account_type in ('AUTO', 'PERS')
            then abs(risk_weight - 0.75) > 0.001
        when account_type = 'CC'
            then abs(risk_weight - 0.75) > 0.001
        -- Source-faithful: LOC explicitly 1.00 in SAS source
        when account_type = 'LOC'
            then abs(risk_weight - 1.00) > 0.001
        else abs(risk_weight - 1.00) > 0.001
    end
