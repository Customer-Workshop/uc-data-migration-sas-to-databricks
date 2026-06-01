/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA — Basel III standardized-approach
    Risk-Weighted Assets by account type and customer segment. Reads the daily
    account snapshot STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN ORA_DW.LOAN_DETAILS
    for the LTV used to band mortgages, filtered to SNAPSHOT_DATE = month_end.

  dbt Equivalent:
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY (the dbt daily
    account snapshot). The CASE expression reproduces the SAS risk-weight
    mapping value-for-value, including the catch-all else. Per-value fidelity is
    gated by tests/reconcile_rwa_risk_weight_parity.sql.

  Data-source mapping note (flagged, not a logic change):
    - The SAS LTV comes from ORA_DW.LOAN_DETAILS.LTV. The Databricks raw schema
      has no stored ltv column, so LTV is reconstructed as
      current_balance / collateral_value from raw.collateral (loan-to-value by
      definition). An MTG account with no collateral row (NULL/0 collateral
      value) yields a NULL LTV and therefore fails both MTG bands and falls to
      else -> 1.00, exactly as a missing SAS LEFT JOIN match would.

  Source-faithful quirks reproduced (flagged, NOT corrected — any change is a
  separate, deliberate business decision):
    - LOC (line of credit) is mapped explicitly to 1.00, NOT 0.75 like the other
      revolving products CC/PERS. Reproduced exactly as in the SAS source. (This
      is the canonical conversion trap: "fixing" LOC to 0.75 silently overstates
      capital relief; reconcile_rwa_risk_weight_parity fails if anyone does.)
    - IRA has no explicit branch and falls through the catch-all else -> 1.00,
      i.e. a deposit-like retirement product carries a 100% risk weight. Unusual
      for Basel III but faithful to the source.
    - The SAS SNAPSHOT_DATE = month_end filter has no equivalent here: the dbt
      snapshot is point-in-time (no time-series snapshots are materialized).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select
        account_id,
        collateral_value
    from {{ source('banking_raw', 'collateral') }}
),

-- Per-account risk weight — mirrors the SAS CASE in Step 1 exactly.
account_risk as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG'
                and c.collateral_value > 0
                and a.current_balance / c.collateral_value <= 0.80 then 0.35
            when a.account_type = 'MTG'
                and c.collateral_value > 0
                and a.current_balance / c.collateral_value > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            -- SAS: else 1.00 — catches IRA, MTG with no LTV, and any unmapped
            -- type (source-faithful).
            else 1.00
        end as risk_weight
    from accounts a
    left join collateral c
        on a.account_id = c.account_id
),

-- Aggregate to the reporting grain (SAS: group by 1,2,3,4).
grouped as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        customer_segment,
        risk_weight,
        count(*) as n_accounts,
        sum(current_balance) as total_exposure,
        sum(current_balance * risk_weight) as rwa
    from account_risk
    group by
        report_month,
        account_type,
        customer_segment,
        risk_weight
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    n_accounts,
    total_exposure,
    rwa
from grouped
