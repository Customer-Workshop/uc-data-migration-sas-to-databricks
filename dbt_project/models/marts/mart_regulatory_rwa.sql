/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)
  Output contract: REPORTS.MONTHLY_RWA (replaced each run)

  SAS Original:
    Basel III standardised risk weights applied to the month-end snapshot
    (SNAPSHOT_DATE = "&month_end"d) LEFT JOIN ORA_DW.LOAN_DETAILS for LTV:
      CHK/SAV/MMA            0.00
      CD                     0.00
      MTG and LTV <= 0.80    0.35
      MTG and LTV >  0.80    0.50
      HELC                   0.50
      AUTO/PERS              0.75
      CC                     0.75
      LOC                    1.00
      else                   1.00
    N_ACCOUNTS = count(*), TOTAL_EXPOSURE = sum(CURRENT_BALANCE),
    RWA = sum(CURRENT_BALANCE * RISK_WEIGHT)
    group by REPORT_MONTH, ACCOUNT_TYPE, CUSTOMER_SEGMENT, RISK_WEIGHT.

  SAS quirk preserved:
    SAS SQL orders a missing value below every number, so an MTG account with
    no LOAN_DETAILS row (LTV missing) satisfies `LTV <= 0.80` and gets 0.35.
    The CASE below is null-aware to reproduce that.

  Period: report_month var (default &PREV_YM); month_end = last_day(month_start).
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
),

loans as (
    select * from {{ ref('stg_loan_details') }}
),

weighted as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
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
    '{{ sas_report_month() }}' as report_month,
    account_type,
    customer_segment,
    risk_weight,
    count(*) as n_accounts,
    sum(current_balance) as total_exposure,
    sum(current_balance * risk_weight) as rwa,
    {{ format_account_type('account_type') }} as account_type_desc,
    {{ format_customer_segment('customer_segment') }} as customer_segment_desc
from weighted
group by account_type, customer_segment, risk_weight
