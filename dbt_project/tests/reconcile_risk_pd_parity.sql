/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 2 WOE bins,
  coefficients, intercept, and logistic PD are independently reconstructed
  from literal mapping tables. Any row above 1e-9 absolute difference fails.
*/
with fico_bins as (
    select * from values
        (-999999.0, 600.0, 1.102),
        (600.0, 640.0, 0.654),
        (640.0, 680.0, 0.198),
        (680.0, 720.0, -0.356),
        (720.0, 760.0, -0.812),
        (760.0, 999999.0, -1.204)
        as t(lower_bound, upper_bound, value)
),

util_bins as (
    select * from values
        (-999999.0, 10.0, -0.956),
        (10.0, 30.0, -0.521),
        (30.0, 50.0, -0.102),
        (50.0, 70.0, 0.334),
        (70.0, 90.0, 0.789),
        (90.0, 999999.0, 1.245)
        as t(lower_bound, upper_bound, value)
),

dpd_bins as (
    select * from values
        (0, -0.678),
        (1, 0.445)
        as t(dpd, value)
),

age_bins as (
    select * from values
        (-999999.0, 24.0, 0.456),
        (24.0, 60.0, 0.045),
        (60.0, 120.0, -0.289),
        (120.0, 999999.0, -0.534)
        as t(lower_bound, upper_bound, value)
),

ltv_bins as (
    select * from values
        (-999999.0, 0.60, -0.712),
        (0.60, 0.80, -0.234),
        (0.80, 1.00, 0.356),
        (1.00, 999999.0, 0.889)
        as t(lower_bound, upper_bound, value)
),

inputs as (
    select
        s.account_id,
        s.pd,
        b.fico_score,
        a.utilization_pct,
        p.pmt_late_90_12mo,
        a.acct_age_months,
        a.account_type,
        case
            when c.collateral_value > 0
                then a.current_balance / c.collateral_value
        end as ltv
    from {{ ref('mart_risk_scores') }} s
    inner join {{ ref('int_account_metrics') }} a
        on s.account_id = a.account_id
        and a.snapshot_date = current_date()
    left join {{ source('banking_raw', 'bureau_scores') }} b
        on a.customer_id = b.customer_id
    left join {{ source('banking_raw', 'payment_history') }} p
        on a.account_id = p.account_id
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

expected as (
    select
        i.account_id,
        1.0 / (
            1.0 + exp(-(
                -3.2145
                + 0.412 * case
                    when i.fico_score is null then 0.198
                    else coalesce(f.value, 1.102)
                end
                + 0.198 * case
                    when i.utilization_pct is null then 0
                    else coalesce(u.value, 1.245)
                end
                + 0.289 * case
                    when i.pmt_late_90_12mo is null then 0
                    else coalesce(d.value, 1.567)
                end
                + 0.067 * case
                    when i.acct_age_months is null then 0
                    else coalesce(g.value, 0.456)
                end
                + 0.134 * case
                    when i.account_type in ('MTG', 'AUTO', 'HELC')
                        then case
                            when i.ltv is null then 0
                            else coalesce(l.value, 0.889)
                        end
                    else 0
                end
            ))
        ) as expected_pd
    from inputs i
    left join fico_bins f
        on i.fico_score >= f.lower_bound
        and i.fico_score < f.upper_bound
    left join util_bins u
        on i.utilization_pct > u.lower_bound
        and i.utilization_pct <= u.upper_bound
    left join dpd_bins d on i.pmt_late_90_12mo = d.dpd
    left join age_bins g
        on i.acct_age_months >= g.lower_bound
        and i.acct_age_months < g.upper_bound
    left join ltv_bins l
        on i.ltv > l.lower_bound
        and i.ltv <= l.upper_bound
)

select
    e.account_id,
    e.expected_pd,
    s.pd,
    abs(s.pd - e.expected_pd) as abs_diff
from expected e
inner join {{ ref('mart_risk_scores') }} s on e.account_id = s.account_id
where abs(s.pd - e.expected_pd) > 1e-9
