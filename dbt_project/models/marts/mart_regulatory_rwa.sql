/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA — Basel III standardized approach
    risk weights aggregated by account type and customer segment.

    Inputs:  STG_BANK.CUST_ACCOUNTS_DAILY LEFT JOIN ORA_DW.LOAN_DETAILS
    Filter:  SNAPSHOT_DATE = "&month_end"d
    Group:   REPORT_MONTH, ACCOUNT_TYPE, CUSTOMER_SEGMENT, RISK_WEIGHT

  dbt Equivalent:
    int_account_metrics replaces STG_BANK.CUST_ACCOUNTS_DAILY (already a
    point-in-time snapshot via snapshot_date = current_date()).
    source('banking_raw', 'collateral') provides collateral_value for LTV
    derivation (SAS had LTV pre-computed on ORA_DW.LOAN_DETAILS — schema
    adaptation flagged below).

  Source-faithful notes:
    - LOC → 1.00 is explicit in the SAS CASE; reproduced here verbatim.
    - IRA has no explicit branch; falls through to else → 1.00.
      (See docs/CONVERSION_PLAYBOOK.md LOC worked example.)
    - LTV is derived as current_balance / collateral_value (SAS had it
      pre-computed on LOAN_DETAILS). Functionally equivalent.
    - MTG accounts with NULL LTV (no collateral record) fall through to
      else → 1.00, matching SAS LEFT JOIN behaviour where LOAN_DETAILS
      is absent.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

collateral as (
    select * from {{ source('banking_raw', 'collateral') }}
),

-- Per-account risk-weight assignment (pre-aggregation).
-- Reproduces SAS CASE value for value; see source-faithful notes above.
account_risk as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG'
                 and c.collateral_value is not null
                 and c.collateral_value > 0
                 and (a.current_balance / c.collateral_value) <= 0.80
                then 0.35
            when a.account_type = 'MTG'
                 and c.collateral_value is not null
                 and c.collateral_value > 0
                 and (a.current_balance / c.collateral_value) > 0.80
                then 0.50
            /* SAS: MTG with NULL LTV falls through to else → 1.00 */
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
            /* SAS else → 1.00: catches IRA and any unmapped type */
            else 1.00
        end as risk_weight
    from accounts a
    left join collateral c
        on a.account_id = c.account_id
)

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
