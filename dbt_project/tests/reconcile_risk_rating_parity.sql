/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 2 rating thresholds
  are represented by this independent literal threshold table, including the
  catch-all rating 7 for PD >= 0.30 or null PD.
*/
with thresholds as (
    select * from values
        (0.000, 0.005, 1),
        (0.005, 0.010, 2),
        (0.010, 0.030, 3),
        (0.030, 0.070, 4),
        (0.070, 0.150, 5),
        (0.150, 0.300, 6),
        (0.300, 999999.0, 7)
        as t(lower_bound, upper_bound, expected_rating)
),

expected as (
    select
        s.account_id,
        s.risk_rating,
        coalesce(t.expected_rating, 7) as expected_rating
    from {{ ref('mart_risk_scores') }} s
    left join thresholds t
        on s.pd >= t.lower_bound
        and s.pd < t.upper_bound
)

select
    account_id,
    risk_rating,
    expected_rating
from expected
where risk_rating <> expected_rating
