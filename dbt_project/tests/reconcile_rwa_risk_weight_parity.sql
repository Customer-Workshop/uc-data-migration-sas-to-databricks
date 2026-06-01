/*
  Reconciliation: risk-weight parity per account.

  This is the key migration-fidelity check.  For every account, the
  risk_weight assigned by the mart must match the SAS CASE from
  monthly_regulatory_reporting.sas Step 1 exactly.

  The expected mapping (source of truth):
    CHK/SAV/MMA         -> 0.00
    CD                  -> 0.00
    MTG (LTV <= 0.80)   -> 0.35
    MTG (LTV >  0.80)   -> 0.50
    HELC                -> 0.50
    AUTO/PERS           -> 0.75
    CC                  -> 0.75
    LOC                 -> 1.00   (explicit in SAS, not a catch-all)
    else (incl. IRA)    -> 1.00

  The test recomputes the expected weight for each account and compares
  it to the weight the mart actually assigned.  Fails if any mismatch.
*/
with per_account_expected as (
    select
        a.account_type,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG'
                 and c.collateral_value > 0
                 and a.current_balance / c.collateral_value <= 0.80
                                                          then 0.35
            when a.account_type = 'MTG'                   then 0.50
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
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
        count(*) as n
    from per_account_expected
    group by account_type, expected_rw
),

mart_summary as (
    select
        account_type,
        risk_weight as actual_rw,
        sum(n_accounts) as n
    from {{ ref('mart_regulatory_rwa') }}
    group by account_type, risk_weight
)

select
    coalesce(e.account_type, m.account_type) as account_type,
    e.expected_rw,
    m.actual_rw,
    e.n as expected_n,
    m.n as actual_n
from expected_summary e
full outer join mart_summary m
    on e.account_type = m.account_type
    and abs(e.expected_rw - m.actual_rw) < 0.001
where e.account_type is null
   or m.account_type is null
   or e.n <> m.n
