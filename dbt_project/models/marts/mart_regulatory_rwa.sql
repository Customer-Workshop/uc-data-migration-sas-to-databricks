/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA — Basel III standardized-approach
    Risk-Weighted Assets by account type and customer segment. Reads the daily
    account snapshot STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN ORA_DW.LOAN_DETAILS
    (for the LTV used to band mortgages), filtered to SNAPSHOT_DATE = month_end.

  dbt Equivalent:
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY (the dbt daily
    account snapshot). banking_raw.loan_details replaces ORA_DW.LOAN_DETAILS and
    supplies the stored ltv column the SAS join reads. The CASE expression
    reproduces the SAS risk-weight mapping value-for-value, including the
    catch-all else. Per-value fidelity is gated by
    tests/reconcile_rwa_risk_weight_parity.sql.

  Source-faithful quirks reproduced (flagged, NOT corrected here — any change is
  a separate, deliberate business decision):
    - LOC (line of credit) is mapped explicitly to 1.00, NOT 0.75 like the other
      revolving products CC/PERS. Reproduced exactly as in the SAS source. (This
      is the canonical conversion trap: "fixing" LOC to 0.75 would silently
      overstate capital relief; the risk-weight parity control fails if anyone
      does.)
    - IRA has no explicit branch and falls through the catch-all else -> 1.00,
      i.e. a deposit-like retirement product carries a 100% risk weight. Unusual
      for Basel III but faithful to the source.
    - MTG rows without a loan_details/ltv record fall through to else -> 1.00
      (the SAS LEFT JOIN yields a missing LTV which fails both MTG bands). In the
      current raw data every MTG account has an ltv, so this branch does not fire.
    - The SAS SNAPSHOT_DATE = month_end filter has no equivalent here: the dbt
      snapshot is point-in-time (no time-series snapshots are materialized).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
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
            when a.account_type = 'MTG' and l.ltv <= 0.80 then 0.35
            when a.account_type = 'MTG' and l.ltv > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            -- SAS: else 1.00 — catches IRA and any unmapped type (source-faithful).
            else 1.00
        end as risk_weight
    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
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
