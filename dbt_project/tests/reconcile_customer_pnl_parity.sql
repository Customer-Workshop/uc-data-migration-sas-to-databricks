/*
  Reconciliation control (per-value parity): customer_profitability.sas

  Aggregate totals can tie out while an individual branch is wrong, so this
  control checks each mapping/CASE in the program value-for-value:

    A. account-type coverage — every account type present in the source must be
       classified by the SAS lending OR deposit set. A type that matches neither
       falls through both CASE arms to "else 0" and silently earns no interest
       (the customer_profitability analogue of the LOC risk-weight divergence in
       the playbook). Any uncovered type is surfaced here.
    B. net_interest_income parity — re-derive NII per customer from the source
       and compare to the mart value-for-value (catches a single misclassified
       account type even when the grand total happens to net out).
    C. profit_tier parity — recompute the tier from net_profit and confirm it
       matches the mart's stored tier (the SAS IF/THEN thresholds).

  dbt singular test convention: FAILS if this query returns any rows.
*/
with src_nii as (
    select
        customer_id,
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
    group by customer_id
)

-- A. account types not covered by either SAS classification set
select
    'account_type_uncovered' as check_name,
    account_type as detail
from {{ ref('stg_cust_accounts') }}
where account_type not in (
    'MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC',
    'CHK', 'SAV', 'MMA', 'CD', 'IRA'
)
group by account_type

union all

-- B. per-customer net_interest_income must match the source re-derivation
select
    'nii_parity' as check_name,
    cast(m.customer_id as string) as detail
from {{ ref('mart_customer_pnl') }} m
inner join src_nii s
    on m.customer_id = s.customer_id
where abs(m.net_interest_income - s.net_interest_income) > 0.01

union all

-- C. profit_tier must match the SAS IF/THEN thresholds applied to net_profit
select
    'profit_tier_parity' as check_name,
    cast(customer_id as string) as detail
from {{ ref('mart_customer_pnl') }}
where profit_tier <> case
    when net_profit >= 500 then 'Highly Profitable'
    when net_profit >= 100 then 'Profitable'
    when net_profit >= 0 then 'Marginal'
    else 'Unprofitable'
end
