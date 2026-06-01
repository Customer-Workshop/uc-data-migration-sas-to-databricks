/*
  Reconciliation: account-type CASE parity for interest income.

  The SAS program classifies account types into two buckets:
    Lending:  MTG, AUTO, PERS, CC, LOC, HELC
    Deposit:  CHK, SAV, MMA, CD, IRA

  Any account_type not in either list falls through to ELSE 0 in both CASEs,
  contributing nothing to lending_income or deposit_cost.  This is a silent
  pass-through — flagged as quirk Q5.

  This parity check surfaces any account_type in the raw data that is NOT
  covered by either bucket.  Returning rows does not necessarily mean the
  model is wrong (the SAS source has the same gap), but it highlights
  unmapped values so the business can decide whether to act.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    account_type,
    count(*) as n_accounts
from {{ ref('stg_cust_accounts') }}
where account_type not in (
    /* Lending */
    'MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC',
    /* Deposit */
    'CHK', 'SAV', 'MMA', 'CD', 'IRA'
)
group by account_type
