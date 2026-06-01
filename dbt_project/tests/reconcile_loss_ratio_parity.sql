/*
  Reconciliation control: per-value parity for the mart aggregates and the
  $POLTYPE format mapping (policy_valuation.sas Step 5 + Formats/insurance_formats.sas).

  Checks, value-for-value for EVERY line of business:
    1. AGG_LOSS_RATIO     = TOTAL_INCURRED / TOTAL_EARNED   (when TOTAL_EARNED > 0)
    2. AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30           (when TOTAL_EARNED > 0)
    3. policy_type_desc matches the legacy $POLTYPE catalog, restated literally
       below (independent of format_policy_type so the test catches macro drift).
       The catch-all is 'Unknown' (OTHER) — codes absent from the catalog
       (LIFE / HEALTH / COMMERCIAL) decode to 'Unknown'. This is the documented,
       source-faithful quirk, not a defect: the parity check PROVES the mart
       reproduces $POLTYPE exactly rather than silently re-mapping it.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart as (
    select * from {{ ref('mart_loss_ratios') }}
),

expected_poltype as (
    select code, label from values
        ('WL', 'Whole Life'),
        ('TL', 'Term Life'),
        ('UL', 'Universal Life'),
        ('VL', 'Variable Life'),
        ('AUTO', 'Auto Insurance'),
        ('HOME', 'Homeowners'),
        ('RENT', 'Renters'),
        ('UMBR', 'Umbrella'),
        ('HLTH', 'Health'),
        ('DNTL', 'Dental'),
        ('VIS', 'Vision'),
        ('DISAB', 'Disability'),
        ('LTCI', 'Long-Term Care')
        as t (code, label)
)

select
    m.policy_type,
    m.policy_type_desc,
    m.total_earned,
    m.total_incurred,
    m.agg_loss_ratio,
    m.agg_combined_ratio
from mart m
left join expected_poltype e
    on m.policy_type = e.code
where
    -- 1. aggregate loss ratio parity
    (
        m.total_earned > 0
        and abs(coalesce(m.agg_loss_ratio, 0)
            - coalesce(m.total_incurred, 0) / m.total_earned) > 1e-9
    )
    or (m.total_earned > 0 and m.agg_loss_ratio is null and m.total_incurred is not null)

    -- 2. aggregate combined ratio = aggregate loss ratio + 0.30
    or (
        m.total_earned > 0
        and abs(coalesce(m.agg_combined_ratio, 0)
            - (coalesce(m.agg_loss_ratio, 0) + 0.30)) > 1e-9
    )

    -- 3. $POLTYPE mapping parity (catch-all 'Unknown' for codes not in catalog)
    or m.policy_type_desc <> coalesce(e.label, 'Unknown')
