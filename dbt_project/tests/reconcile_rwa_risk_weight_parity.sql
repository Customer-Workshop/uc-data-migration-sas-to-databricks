/*
  Reconciliation control: risk-weight parity, per value.

  This is the key migration-fidelity check for Step 1. Parity is per-value, not
  aggregate: a mapping can produce a correct grand total while an individual
  branch is wrong. For every account, the risk weight the mart assigned must
  match the SAS CASE from monthly_regulatory_reporting.sas value-for-value.

  Expected mapping (SAS source of truth):
    CHK/SAV/MMA        -> 0.00
    CD                 -> 0.00
    MTG (ltv <= 0.80)  -> 0.35
    MTG (ltv >  0.80)  -> 0.50
    HELC               -> 0.50
    AUTO/PERS          -> 0.75
    CC                 -> 0.75
    LOC                -> 1.00   (explicit in SAS — NOT 0.75 like CC/PERS)
    else (incl. IRA)   -> 1.00

  The expected weight is recomputed per account from the raw inputs and compared
  to the mart at (account_type, risk_weight) grain. A mismatch (e.g. LOC mapped
  to 0.75) leaves an unmatched row on one side of the full outer join and fails.
  LTV is reconstructed as current_balance / collateral_value from raw.collateral
  (the Databricks raw schema has no stored ltv column — see mart header).

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_per_account as (
    select
        a.account_type,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG'
                and c.collateral_value > 0
                and a.current_balance / c.collateral_value <= 0.80 then 0.35
            when a.account_type = 'MTG'
                and c.collateral_value > 0
                and a.current_balance / c.collateral_value > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end as expected_rw
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected_summary as (
    select
        account_type,
        expected_rw,
        count(*) as n_expected
    from expected_per_account
    group by account_type, expected_rw
),

mart_summary as (
    select
        account_type,
        risk_weight as actual_rw,
        sum(n_accounts) as n_actual
    from {{ ref('mart_regulatory_rwa') }}
    group by account_type, risk_weight
)

select
    coalesce(e.account_type, m.account_type) as account_type,
    e.expected_rw,
    m.actual_rw,
    e.n_expected,
    m.n_actual
from expected_summary e
full outer join mart_summary m
    on e.account_type = m.account_type
    and abs(e.expected_rw - m.actual_rw) < 0.0001
where e.account_type is null
   or m.account_type is null
   or coalesce(e.n_expected, -1) <> coalesce(m.n_actual, -1)
