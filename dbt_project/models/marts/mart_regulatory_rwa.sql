/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL creating REPORTS.MONTHLY_RWA from STG_BANK.CUST_ACCOUNTS_DAILY
    LEFT JOIN ORA_DW.LOAN_DETAILS, filtered to the month-end snapshot, with a
    CASE assigning Basel III standardized-approach risk weights and a
    GROUP BY 1,2,3,4 (report month, account type, segment, risk weight).

  dbt Equivalent:
    int_account_metrics is the converted CUST_ACCOUNTS_DAILY snapshot; the
    `loan_details` source replaces the Oracle LIBNAME; the SAS `calculated
    RISK_WEIGHT` reference becomes a CTE so the weight can be reused in the
    aggregate and in the GROUP BY.

  Source parity notes (see reconcile_rwa_* tests):
    - The risk-weight CASE mirrors the SAS branches value-for-value, including
      the explicit LOC -> 1.00 branch and the catch-all else -> 1.00 (which is
      what IRA falls through to). LOC is *not* grouped with the other revolving
      products at 0.75: that would diverge from the source and overstate
      capital relief.
    - SAS treats a missing LTV as smaller than any number, so a MTG row with no
      LTV on LOAN_DETAILS satisfies `LTV <= 0.80` and is weighted 0.35. SQL
      would send NULL to the catch-all (1.00) instead, so the null is matched
      explicitly. Source-faithful, not an endorsement.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
    -- SAS: where a.SNAPSHOT_DATE = "&month_end"d — the converted staging layer
    -- carries a single current snapshot, which is that month-end position.
    where snapshot_date = current_date()
),

loans as (
    select * from {{ source('banking_raw', 'loan_details') }}
),

weighted as (
    select
        a.account_type,
        a.customer_segment,
        a.current_balance,

        -- SAS: CASE ... end as RISK_WEIGHT
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG' and (l.ltv is null or l.ltv <= 0.80) then 0.35
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
group by report_month, account_type, customer_segment, risk_weight
order by account_type, customer_segment
