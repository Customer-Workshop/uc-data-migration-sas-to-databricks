/*
  Reconciliation test: Basel III risk-weight parity, branch by branch.
  Program: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  The mart's risk weight per account type is compared against the SAS CASE
  mapping written out as a literal table — the source of truth. Every branch is
  checked value-for-value, including:
    * LOC -> 1.00 (its own explicit branch; NOT 0.75 like the other revolving
      products CC/PERS — "fixing" it to 0.75 diverges from the source and
      understates risk-weighted assets on every line of credit);
    * IRA and any other unmapped type -> 1.00 via the catch-all else;
    * MTG split at LTV 0.80 (0.35 / 0.50), with a missing LTV taking the
      <= 0.80 branch because SAS orders missing numerics below every number.

  Account counts are compared alongside the weights, so a branch that produces
  the right weight for the wrong population also fails.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_accounts as (
    select
        a.account_type,
        l.ltv
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.snapshot_date = {{ sas_month_end() }}
),

sas_mapping as (
    select *
    from (
        values
        ('CHK', 'any', 0.00),
        ('SAV', 'any', 0.00),
        ('MMA', 'any', 0.00),
        ('CD', 'any', 0.00),
        ('MTG', 'LTV<=0.80', 0.35),
        ('MTG', 'LTV>0.80', 0.50),
        ('HELC', 'any', 0.50),
        ('AUTO', 'any', 0.75),
        ('PERS', 'any', 0.75),
        ('CC', 'any', 0.75),
        ('LOC', 'any', 1.00)
    ) as m (account_type, ltv_band, expected_risk_weight)
),

expected as (
    select
        s.account_type,
        coalesce(m.expected_risk_weight, 1.00) as risk_weight,
        count(*) as n_accounts
    from source_accounts s
    left join sas_mapping m
        on s.account_type = m.account_type
        and (
            m.ltv_band = 'any'
            or (m.ltv_band = 'LTV<=0.80' and (s.ltv is null or s.ltv <= 0.80))
            or (m.ltv_band = 'LTV>0.80' and s.ltv > 0.80)
        )
    group by s.account_type, coalesce(m.expected_risk_weight, 1.00)
),

reported as (
    select
        account_type,
        risk_weight,
        sum(n_accounts) as n_accounts
    from {{ ref('mart_regulatory_rwa') }}
    group by account_type, risk_weight
)

select
    coalesce(e.account_type, r.account_type) as account_type,
    e.risk_weight as sas_risk_weight,
    r.risk_weight as mart_risk_weight,
    e.n_accounts as sas_n_accounts,
    r.n_accounts as mart_n_accounts
from expected e
full outer join reported r
    on e.account_type = r.account_type
    and e.risk_weight = r.risk_weight
where e.risk_weight is null
   or r.risk_weight is null
   or e.n_accounts <> r.n_accounts
