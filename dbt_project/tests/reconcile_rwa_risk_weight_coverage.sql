/*
  Reconciliation test: Basel risk-weight PARITY with the SAS source.

  The risk weights in mart_regulatory_rwa must match, account-type for
  account-type, the CASE defined in the legacy SAS program
  Programs/Banking/monthly_regulatory_reporting.sas. This is a *source-parity*
  contract: a migration's job is to reproduce the SAS numbers faithfully, not to
  "improve" them.

  Why this matters (and the real bug it guards against):
    The SAS CASE maps LOC -> 1.00 explicitly and leaves IRA on the else -> 1.00
    branch. A plausible-looking conversion choice — e.g. mapping LOC -> 0.75 to
    match the other revolving-credit products (CC/PERS) — is wrong because it
    diverges from the source of truth. "Looks reasonable" review does not catch
    that; a parity check against the source does. Remediating a legacy quirk is
    a separate, deliberate decision, never a silent side effect of conversion.

  MTG is LTV-dependent in the source (0.35 at/under 80% LTV, 0.50 above), so it
  is validated by allowed-set rather than a single value.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select * from (values
        ('CHK', 0.00),
        ('SAV', 0.00),
        ('MMA', 0.00),
        ('CD',  0.00),
        ('HELC', 0.50),
        ('AUTO', 0.75),
        ('PERS', 0.75),
        ('CC',   0.75),
        ('LOC',  1.00),
        ('IRA',  1.00)
    ) as t (account_type, risk_weight)
),

actual as (
    select distinct account_type, risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

-- Non-MTG types: the weight must equal the SAS-defined value, and every type
-- present in the mart must be enumerated in the expected contract (a new,
-- unmapped type surfaces here instead of silently inheriting the catch-all).
select
    a.account_type,
    a.risk_weight as actual_weight,
    e.risk_weight as expected_weight
from actual a
left join expected e on a.account_type = e.account_type
where a.account_type <> 'MTG'
  and (e.account_type is null or a.risk_weight <> e.risk_weight)

union all

-- MTG must land in the LTV-dependent allowed set from the source.
select
    a.account_type,
    a.risk_weight as actual_weight,
    cast(null as decimal(5, 2)) as expected_weight
from actual a
where a.account_type = 'MTG'
  and a.risk_weight not in (0.35, 0.50)
