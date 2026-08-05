/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS reads STG_BANK.CUST_ACCOUNTS_DAILY at the month-end snapshot. The
  migrated int_account_metrics relation contains one current daily snapshot,
  so the SAS SNAPSHOT_DATE = "&month_end"d predicate collapses to that
  snapshot and no additional date filter is required here.

  SAS reads ORA_DW.LOAN_DETAILS.LTV, but LTV is not present in the migrated
  raw estate. Following mart_risk_scores, LTV is derived from the collateral
  source as current_balance / collateral_value when collateral_value > 0.

  SAS-to-dbt migration gaps:
    - REPORTS.LLP_COVERAGE (SAS Step 3) is not converted faithfully because
      ALLOWANCE_AMT and DAYS_PAST_DUE are unavailable in the raw estate.
    - PROC EXPORT to XLSX (SAS Step 4) is out of scope; see
      docs/SAS_TO_DBT_MIGRATION_MAP.md.
*/

with account_ltv as (
    select
        '{{ var("prev_ym") }}' as report_month,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when c.collateral_value > 0
            then a.current_balance / c.collateral_value
            else null
        end as ltv
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

weighted_accounts as (
    select
        report_month,
        account_type,
        customer_segment,
        current_balance,
        /*
          SAS numeric missing values compare less than any number. Therefore,
          a MTG account with missing LTV satisfies LTV <= 0.80 and receives
          0.35. This explicit null branch is source-faithful, not an
          endorsement of the legacy missing-value behavior.
        */
        case
            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when account_type = 'CD' then 0.00
            when account_type = 'MTG' and (ltv <= 0.80 or ltv is null) then 0.35
            when account_type = 'MTG' and ltv > 0.80 then 0.50
            when account_type = 'HELC' then 0.50
            when account_type in ('AUTO', 'PERS') then 0.75
            when account_type = 'CC' then 0.75
            when account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from account_ltv
)

select
    report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from weighted_accounts
group by report_month, account_type, customer_segment, risk_weight
order by account_type, customer_segment
