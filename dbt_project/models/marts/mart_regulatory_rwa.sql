/*
  mart_regulatory_rwa.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 1)

  SAS Original:
    PROC SQL with Basel III standardized risk weights applied
    per account type + LTV, aggregated by type and segment.
    SAS "calculated" keyword → nested CTE for column reuse.

  dbt Equivalent:
    CASE assigns risk weight per row, CTE makes it referenceable
    in the GROUP BY aggregation. SAS macro &report_month → dbt var.
    LTV derived from collateral table (current_balance / collateral_value).
*/

with accounts_with_collateral as (
    select
        a.account_id,
        a.account_type,
        a.customer_segment,
        a.current_balance,
        case
            when c.collateral_value > 0
                then abs(a.current_balance) / c.collateral_value
            else null
        end as ltv,
        case
            when a.account_type in ('CHK', 'SAV', 'MMA') then 0.00
            when a.account_type = 'CD'                    then 0.00
            when a.account_type = 'MTG'
                 and c.collateral_value > 0
                 and abs(a.current_balance) / c.collateral_value <= 0.80
                then 0.35
            when a.account_type = 'MTG'                   then 0.50
            when a.account_type = 'HELC'                  then 0.50
            when a.account_type in ('AUTO', 'PERS')       then 0.75
            when a.account_type = 'CC'                    then 0.75
            when a.account_type = 'LOC'                   then 1.00
            else 1.00
        end as risk_weight
    from {{ ref('int_account_metrics') }} a
    left join {{ source('banking_raw', 'collateral') }} c
        on a.account_id = c.account_id
),

risk_weighted as (
    select
        '{{ var("prev_ym") }}' as report_month,
        account_type,
        customer_segment,
        risk_weight,
        count(*) as n_accounts,
        sum(current_balance) as total_exposure,
        sum(current_balance * risk_weight) as rwa
    from accounts_with_collateral
    group by 1, 2, 3, 4
)

select * from risk_weighted
