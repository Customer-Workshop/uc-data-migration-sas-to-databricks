/*
  Reconciliation control: per-account-type risk weight parity with the SAS source.

  Parity is per value, not aggregate: the mart can produce a plausible total RWA
  while an individual CASE branch is wrong. This control pins every fixed-weight
  branch of monthly_regulatory_reporting.sas Step 1 to the weight the SAS program
  assigns, transcribed from the source:

      CHK/SAV/MMA -> 0.00      AUTO/PERS -> 0.75
      CD          -> 0.00      CC        -> 0.75
      HELC        -> 0.50      LOC       -> 1.00   (explicit branch in the SAS)
      IRA         -> 1.00      (falls through to the catch-all `else 1.00`)

  LOC is the branch this control exists for: grouping it with the other
  revolving products at 0.75 looks reasonable and is wrong — it diverges from
  the source and overstates capital relief on every line of credit.

  MTG is LTV-dependent and is covered by reconcile_rwa_mtg_ltv_parity.sql.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with sas_mapping as (
    select *
    from (
        values
        ('CHK', 0.00),
        ('SAV', 0.00),
        ('MMA', 0.00),
        ('CD', 0.00),
        ('HELC', 0.50),
        ('AUTO', 0.75),
        ('PERS', 0.75),
        ('CC', 0.75),
        ('LOC', 1.00),
        ('IRA', 1.00)
    ) as t (account_type, expected_weight)
),

mart_weights as (
    select distinct
        account_type,
        risk_weight
    from {{ ref('mart_regulatory_rwa') }}
)

select
    m.account_type,
    m.risk_weight as actual_weight,
    s.expected_weight
from mart_weights m
inner join sas_mapping s
    on m.account_type = s.account_type
where cast(m.risk_weight as decimal(5, 2)) <> cast(s.expected_weight as decimal(5, 2))
