/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL aggregation over STG_BANK.CUST_ACCOUNTS_DAILY left joined to
    ORA_DW.LOAN_DETAILS, assigning Basel III standardized risk weights via
    CASE and grouping by REPORT_MONTH / ACCOUNT_TYPE / CUSTOMER_SEGMENT /
    RISK_WEIGHT.

  dbt Equivalent:
    int_account_metrics is the CUST_ACCOUNTS_DAILY equivalent (the
    SNAPSHOT_DATE = month-end filter selects the single current snapshot,
    which is what int_account_metrics materializes).
    The CASE mirrors the SAS mapping value-for-value, including the
    catch-all else -> 1.00 (IRA and any unmapped type land there).

  Source-faithful quirks (do not "fix" without a business decision):
    - LOC is explicitly mapped to 1.00, NOT 0.75 like the other revolving
      products (CC/PERS). This matches the SAS source.
    - In SAS, a missing LTV compares as lower than any number, so an MTG
      with missing LTV satisfies "LTV <= 0.80" and gets 0.35. The null
      check below reproduces that; plain SQL "null <= 0.80" would instead
      fall through to the 1.00 catch-all and diverge from the source.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

loans as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

weighted as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            -- SAS: missing LTV <= 0.80 is TRUE (missing sorts low)
            when a.account_type = 'MTG' and (l.ltv <= 0.80 or l.ltv is null) then 0.35
            when a.account_type = 'MTG' and l.ltv > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO', 'PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end as risk_weight
    from accounts a
    left join loans l
        on a.account_id = l.account_id
)

select
    '{{ var("prev_ym") }}' as report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa
from weighted
group by account_type, customer_segment, risk_weight
