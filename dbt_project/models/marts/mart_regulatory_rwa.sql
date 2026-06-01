/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA — Basel III standardized approach
    risk weights by account type and customer segment. Joins
    STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS for LTV lookup.

  dbt Equivalent:
    Joins int_account_metrics (replacing STG_BANK.CUST_ACCOUNTS_DAILY) to
    raw loan_details (replacing ORA_DW.LOAN_DETAILS). CASE expression
    reproduces the SAS risk-weight mapping value-for-value.

  Quirks reproduced from source (flagged, not fixed):
    - IRA has no explicit CASE branch and falls through to else -> 1.00.
      This means IRA retirement accounts carry the same risk weight as
      unsecured lines of credit. Likely a source defect — Basel III
      typically assigns 0% to deposit-like products — but reproduced
      faithfully per the conversion playbook.
    - LOC is explicitly mapped to 1.00 (not 0.75 like other revolving
      products CC/PERS). This is intentional in the source.
    - MTG accounts without a loan_details record (NULL LTV) fall through
      to else -> 1.00. The seed data always provides loan_details for MTG,
      so this branch should not fire in practice.
    - The SAS filtered by SNAPSHOT_DATE = month_end; the dbt model uses
      the current point-in-time view (no time-series snapshots available).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

-- Derive per-account risk weight (mirrors the SAS CASE exactly)
account_risk as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG' and l.ltv <= 0.80 then 0.35
            when a.account_type = 'MTG' and l.ltv > 0.80  then 0.50
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
            -- SAS: else 1.00 — catches IRA and any unmapped types.
            -- [QUIRK] IRA falls here; see header comment.
            else 1.00
        end as risk_weight
    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
),

-- Aggregate to the reporting grain (mirrors SAS GROUP BY 1,2,3,4)
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
    order by account_type, customer_segment
)

select * from grouped
