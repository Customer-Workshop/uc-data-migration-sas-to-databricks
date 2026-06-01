/*
  Reconciliation control (control totals): customer_profitability.sas

  Two SUMs computed independently from the source must tie out to the mart:
    1. TOTAL_RELATIONSHIP — sum of CURRENT_BALANCE across the in-scope account
       master (SAS Step 1: sum(a.CURRENT_BALANCE)).
    2. NET_INTEREST_INCOME — sum of (lending interest - deposit interest) across
       the same accounts (SAS Step 1: LENDING_INCOME - DEPOSIT_COST).

  A divergence here means the conversion dropped/duplicated balances or
  misclassified an account type between the lending and deposit CASE arms.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with source_totals as (
    select
        sum(current_balance) as total_relationship,
        sum(
            case
                when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) - sum(
            case
                when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                    then current_balance * interest_rate / 12
                else 0
            end
        ) as net_interest_income
    from {{ ref('stg_cust_accounts') }}
),

mart_totals as (
    select
        sum(total_relationship) as total_relationship,
        sum(net_interest_income) as net_interest_income
    from {{ ref('mart_customer_pnl') }}
)

select
    s.total_relationship as source_relationship,
    m.total_relationship as mart_relationship,
    s.net_interest_income as source_nii,
    m.net_interest_income as mart_nii
from source_totals s
cross join mart_totals m
where abs(s.total_relationship - m.total_relationship) > 0.01
   or abs(s.net_interest_income - m.net_interest_income) > 0.01
