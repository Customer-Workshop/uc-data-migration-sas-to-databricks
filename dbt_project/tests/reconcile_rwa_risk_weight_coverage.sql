/*
  Reconciliation test: RWA risk-weight parity against SAS mapping.

  Every account type's Basel III risk weight must match the SAS CASE from
  monthly_regulatory_reporting.sas (Step 1) value for value.  This test
  fails (returns rows) if any row in mart_regulatory_rwa carries a
  risk_weight that is inconsistent with its account_type.

  Source-of-truth mapping:
    CHK / SAV / MMA   → 0.00
    CD                → 0.00
    MTG               → 0.35 (LTV <= 0.80) | 0.50 (LTV > 0.80) | 1.00 (NULL LTV)
    HELC              → 0.50
    AUTO / PERS       → 0.75
    CC                → 0.75
    LOC               → 1.00
    else              → 1.00

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with rwa as (
    select * from {{ ref('mart_regulatory_rwa') }}
)

select
    account_type,
    risk_weight,
    n_accounts
from rwa
where
    -- CHK / SAV / MMA must be 0.00
    (account_type in ('CHK', 'SAV', 'MMA') and risk_weight != 0.00)
    -- CD must be 0.00
    or (account_type = 'CD' and risk_weight != 0.00)
    -- MTG must be 0.35 or 0.50 or 1.00 (null-LTV catch-all)
    or (account_type = 'MTG' and risk_weight not in (0.35, 0.50, 1.00))
    -- HELC must be 0.50
    or (account_type = 'HELC' and risk_weight != 0.50)
    -- AUTO / PERS must be 0.75
    or (account_type in ('AUTO', 'PERS') and risk_weight != 0.75)
    -- CC must be 0.75
    or (account_type = 'CC' and risk_weight != 0.75)
    -- LOC must be 1.00 (SAS explicitly 1.00, not 0.75)
    or (account_type = 'LOC' and risk_weight != 1.00)
    -- Catch-all types (e.g. IRA) must be 1.00
    or (
        account_type not in ('CHK', 'SAV', 'MMA', 'CD', 'MTG', 'HELC', 'AUTO', 'PERS', 'CC', 'LOC')
        and risk_weight != 1.00
    )
