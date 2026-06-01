/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA
    Basel III standardized approach risk weights by account type / segment.
    Joined STG_BANK.CUST_ACCOUNTS_DAILY to ORA_DW.LOAN_DETAILS for LTV.

  dbt Equivalent:
    SQL CASE replaces SAS CASE for risk-weight assignment.
    LTV derived from collateral table (SAS carried LTV on LOAN_DETAILS directly).
    stg_cust_accounts replaces STG_BANK.CUST_ACCOUNTS_DAILY.
    SAS SNAPSHOT_DATE filter omitted — staging layer represents current state.

  Risk-weight mapping (source-faithful, including catch-all):
    CHK / SAV / MMA       → 0.00
    CD                    → 0.00
    MTG  LTV <= 0.80      → 0.35
    MTG  LTV >  0.80      → 0.50
    HELC                  → 0.50
    AUTO / PERS           → 0.75
    CC                    → 0.75
    LOC                   → 1.00   (SAS explicitly 1.00, not 0.75)
    else                  → 1.00
*/

with accounts as (
    select * from {{ ref('stg_cust_accounts') }}
),

collateral as (
    select * from {{ source('banking_raw', 'collateral') }}
),

with_ltv as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        -- LTV derived from collateral; SAS had LTV on LOAN_DETAILS directly.
        -- NULL LTV falls through to the catch-all (1.00), matching SAS NULL semantics.
        case
            when c.collateral_value is not null and c.collateral_value > 0
            then a.current_balance / c.collateral_value
            else null
        end as ltv
    from accounts a
    left join collateral c
        on a.account_id = c.account_id
),

with_risk_weight as (
    select
        *,
        -- Basel III standardized approach — reproduces SAS CASE value for value
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD'                    then 0.00
            when account_type = 'MTG' and ltv <= 0.80   then 0.35
            when account_type = 'MTG' and ltv > 0.80    then 0.50
            when account_type = 'HELC'                  then 0.50
            when account_type in ('AUTO', 'PERS')       then 0.75
            when account_type = 'CC'                    then 0.75
            when account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight
    from with_ltv
)

select
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from with_risk_weight
group by
    report_month,
    account_type,
    customer_segment,
    risk_weight
order by account_type, customer_segment
