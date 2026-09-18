/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA from STG_BANK.CUST_ACCOUNTS_DAILY
    left joined to ORA_DW.LOAN_DETAILS, with the Basel III standardized-approach
    risk weight as a CASE expression, grouped by
    REPORT_MONTH / ACCOUNT_TYPE / CUSTOMER_SEGMENT / RISK_WEIGHT (group by 1,2,3,4).

  dbt Equivalent:
    int_account_metrics is the converted STG_BANK.CUST_ACCOUNTS_DAILY; the
    LOAN_DETAILS join and the CASE mapping are reproduced branch-for-branch.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
),

loan_details as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

joined as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,
        l.ltv
    from accounts a
    left join loan_details l
        on a.account_id = l.account_id
),

risk_weighted as (
    select
        *,
        -- Basel III risk weights, reproduced value-for-value from the SAS CASE.
        -- Source-faithful quirks (not endorsements):
        --   * LOC has its own branch at 1.00 — it is NOT grouped with the other
        --     revolving products (CC/PERS at 0.75).
        --   * IRA has no branch and falls through the catch-all else to 1.00.
        --   * SAS orders missing numerics below every number, so an MTG row with
        --     no LOAN_DETAILS match (LTV = .) satisfies `LTV <= 0.80` and scores
        --     0.35. `ltv is null` keeps that behaviour, which plain SQL NULL
        --     comparison would otherwise send to the catch-all 1.00 branch.
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD' then 0.00
            when account_type = 'MTG' and (ltv is null or ltv <= 0.80) then 0.35
            when account_type = 'MTG' and ltv > 0.80 then 0.50
            when account_type = 'HELC' then 0.50
            when account_type in ('AUTO', 'PERS') then 0.75
            when account_type = 'CC' then 0.75
            when account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from joined
)

select
    {{ sas_report_month() }} as report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from risk_weighted
group by account_type, customer_segment, risk_weight
order by account_type, customer_segment
