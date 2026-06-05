/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA from STG_BANK.CUST_ACCOUNTS_DAILY
    LEFT JOIN ORA_DW.LOAN_DETAILS. Basel III standardized risk weights by
    account type, grouped by REPORT_MONTH, ACCOUNT_TYPE, CUSTOMER_SEGMENT,
    RISK_WEIGHT.

  dbt Equivalent:
    SQL model reading from int_account_metrics (replaces STG_BANK.CUST_ACCOUNTS_DAILY)
    with a left join to the collateral source (for LTV derivation).
    CASE expression mirrors the SAS risk-weight mapping value-for-value.

  Source-faithful notes:
    - LOC explicitly maps to risk weight 1.00 in the SAS source. Do not
      change it to 0.75 to match other revolving products (CC/PERS) — that
      would diverge from the source and overstate capital relief.
    - IRA has no explicit branch in the SAS CASE and falls to the catch-all
      else -> 1.00. Reproduced here as the else branch.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select * from {{ source('banking_raw', 'collateral') }}
),

with_ltv as (
    select
        a.*,
        case
            when c.collateral_value > 0
            then a.current_balance / c.collateral_value
            else null
        end as ltv
    from accounts a
    left join collateral c
        on a.account_id = c.account_id
),

risk_weighted as (
    select
        {{ var('prev_ym') }} as report_month,
        account_type,
        customer_segment,
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD'                    then 0.00
            when account_type = 'MTG' and ltv <= 0.80   then 0.35
            when account_type = 'MTG' and ltv > 0.80    then 0.50
            when account_type = 'HELC'                  then 0.50
            when account_type in ('AUTO', 'PERS')       then 0.75
            when account_type = 'CC'                    then 0.75
            /* SAS source: LOC explicitly -> 1.00 (source-faithful) */
            when account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight,
        current_balance
    from with_ltv
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from risk_weighted
group by
    report_month,
    account_type,
    customer_segment,
    risk_weight
order by account_type, customer_segment
