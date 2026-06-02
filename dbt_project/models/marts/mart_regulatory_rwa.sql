/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL aggregating STG_BANK.CUST_ACCOUNTS_DAILY (left join ORA_DW.LOAN_DETAILS)
    for the month-end snapshot, assigning Basel III standardized-approach risk
    weights per account type, then grouping by ACCOUNT_TYPE, CUSTOMER_SEGMENT and
    RISK_WEIGHT to produce N_ACCOUNTS, TOTAL_EXPOSURE and RWA.

  dbt Equivalent:
    PROC SQL GROUP BY -> dbt SQL model with group by.
    SAS CASE risk-weight assignment is reproduced value-for-value (see below).
    SAS source-of-truth principle: the risk-weight mapping mirrors the SAS CASE
    exactly, including LOC -> 1.00 and the catch-all else -> 1.00. It is NOT
    "improved" (e.g. LOC is NOT mapped to 0.75 to match other revolving products);
    the source-parity control reconcile_rwa_parity.sql gates this.

  Source mapping notes (migrated raw schema vs. SAS Oracle schema):
    - STG_BANK.CUST_ACCOUNTS_DAILY (month-end account snapshot) -> int_account_metrics,
      which already applies the staging scope filter (status not in 'W','C').
    - SAS read LTV from ORA_DW.LOAN_DETAILS; the migrated raw estate has no LTV
      column, so LTV is derived from collateral (current_balance / collateral_value),
      the same convention used by mart_risk_scores.sql.
*/

with accounts as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,

        -- SAS: LTV from ORA_DW.LOAN_DETAILS; derived here from collateral
        --      (same convention as mart_risk_scores.sql). Null when no collateral.
        case
            when c.collateral_value > 0
                then a.current_balance / c.collateral_value
            else null
        end as ltv

    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

-- SAS: Step 1 risk-weight CASE (Basel III standardized approach), reproduced
--      value-for-value. An MTG with a null LTV falls through both MTG branches
--      to the catch-all else -> 1.00, exactly as the SAS CASE does (source-faithful).
risk_weighted as (
    select
        *,
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD' then 0.00
            when account_type = 'MTG' and ltv <= 0.80 then 0.35
            when account_type = 'MTG' and ltv > 0.80 then 0.50
            when account_type = 'HELC' then 0.50
            when account_type in ('AUTO', 'PERS') then 0.75
            when account_type = 'CC' then 0.75
            when account_type = 'LOC' then 1.00  -- SAS: LOC -> 1.00 (do NOT "improve" to 0.75)
            else 1.00
        end as risk_weight
    from accounts
)

select
    -- SAS: "&report_month" (YYYYMM); &PREV_YM -> dbt var prev_ym
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from risk_weighted
group by account_type, customer_segment, risk_weight
order by account_type, customer_segment, risk_weight
