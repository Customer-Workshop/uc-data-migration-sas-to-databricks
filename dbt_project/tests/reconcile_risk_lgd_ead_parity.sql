/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 2 LGD and EAD rules
  are independently reconstructed per account type and LTV. It covers
  secured missing-LTV defaults, the CC special case, the all-other catch-all,
  and the revolving 50% undrawn-limit conversion.
*/
with ltv_inputs as (
    select
        s.account_id,
        s.account_type,
        s.lgd,
        s.ead,
        a.current_balance,
        a.credit_limit,
        case
            when c.collateral_value > 0
                then a.current_balance / c.collateral_value
        end as ltv
    from {{ ref('mart_risk_scores') }} s
    inner join {{ ref('int_account_metrics') }} a
        on s.account_id = a.account_id
        and a.snapshot_date = current_date()
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

account_type_rules as (
    select * from values
        ('MTG', 'SECURED', null),
        ('AUTO', 'SECURED', null),
        ('HELC', 'SECURED', null),
        ('CC', 'FIXED', 0.75),
        ('LOC', 'FIXED', 0.50),
        ('PERS', 'FIXED', 0.50)
        as t(account_type, lgd_mode, fixed_lgd)
),

ead_rules as (
    select * from values
        ('CC', 'REVOLVING'),
        ('LOC', 'REVOLVING'),
        ('HELC', 'REVOLVING'),
        ('MTG', 'DRAWN'),
        ('AUTO', 'DRAWN'),
        ('PERS', 'DRAWN')
        as t(account_type, ead_mode)
),

expected as (
    select
        i.*,
        r.lgd_mode,
        r.fixed_lgd,
        case
            when r.lgd_mode = 'SECURED'
                then coalesce(
                    greatest(0, least(1, (i.ltv - 0.5) * 0.8)),
                    0.40
                )
            else r.fixed_lgd
        end as expected_lgd,
        case
            when e.ead_mode = 'REVOLVING'
                then i.current_balance + 0.50 * (i.credit_limit - i.current_balance)
            else i.current_balance
        end as expected_ead
    from ltv_inputs i
    inner join account_type_rules r on i.account_type = r.account_type
    inner join ead_rules e on i.account_type = e.account_type
)

select
    account_id,
    lgd,
    expected_lgd,
    ead,
    expected_ead
from expected
where abs(lgd - expected_lgd) > 1e-9
   or abs(ead - expected_ead) > 1e-9
