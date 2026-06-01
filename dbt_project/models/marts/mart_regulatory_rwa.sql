/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA — Basel III standardized approach
    risk-weight assignment by account type, with LTV-driven splits for
    mortgages.  Sources: STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN
    ORA_DW.LOAN_DETAILS, filtered to month-end snapshot.

  dbt Equivalent:
    SQL CASE expression reproduces the SAS risk-weight mapping value-for-value.
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY.
    LTV is computed from collateral.collateral_value / current_balance
    (the SAS ORA_DW.LOAN_DETAILS.LTV is not a direct column in the
    Databricks raw schema; it is derived the same way mart_risk_scores
    does it).

  Source-faithful notes:
    - LOC is explicitly mapped to 1.00 in the SAS source even though the
      catch-all else also yields 1.00.  Reproduced here to preserve parity.
    - IRA is NOT listed in the SAS CASE; it falls to else -> 1.00.
      Reproduced faithfully (not "corrected").
*/

with accounts as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance
    from {{ ref('int_account_metrics') }} a
),

collateral_info as (
    select
        account_id,
        collateral_value
    from {{ source('banking_raw', 'collateral') }}
),

rwa_detail as (
    select
        '{{ var("prev_ym") }}' as report_month,
        a.account_type,
        a.customer_segment,
        /* ── Basel III risk-weight CASE — source-faithful reproduction ──
           Every branch mirrors monthly_regulatory_reporting.sas Step 1
           exactly.  LTV is derived from collateral_value (same derivation
           as mart_risk_scores).  See reconciliation test
           reconcile_rwa_risk_weight_parity for per-value verification. */
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
            -- Source-faithful: LOC explicitly 1.00 in SAS (not merged with else)
            when a.account_type = 'LOC'                   then 1.00
            -- IRA and any unknown types fall here (source-faithful)
            else 1.00
        end as risk_weight,
        a.current_balance
    from accounts a
    left join collateral_info c
        on a.account_id = c.account_id
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from rwa_detail
group by
    report_month,
    account_type,
    customer_segment,
    risk_weight
order by account_type, customer_segment
