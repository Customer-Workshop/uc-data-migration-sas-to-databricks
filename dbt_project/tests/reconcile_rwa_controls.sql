/*
  monthly_regulatory_reporting.sas Step 1 (MONTHLY_RWA) and Step 5 (CAPITAL_ADEQUACY)
    - completeness: sum(N_ACCOUNTS) = month-end snapshot rows
    - control total: sum(TOTAL_EXPOSURE) = sum(CURRENT_BALANCE) of the snapshot
    - RWA identity per group: RWA = TOTAL_EXPOSURE * RISK_WEIGHT
    - mapping parity: each (ACCOUNT_TYPE, RISK_WEIGHT) pair is a legal branch
    - CAPITAL_ADEQUACY.TOTAL_RWA = sum(RWA); status flags follow the thresholds
*/
with snapshot as (
    select count(*) as n, sum(current_balance) as bal
    from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_month_end() }}
),

rwa as (
    select * from {{ ref('mart_regulatory_rwa') }}
),

cap as (
    select * from {{ ref('mart_capital_adequacy') }}
),

totals as (
    select 'sum(N_ACCOUNTS) = snapshot rows' as control, cast(s.n as string) as expected, cast(sum(r.n_accounts) as string) as actual
    from rwa r cross join snapshot s group by s.n
    having s.n <> sum(r.n_accounts)
    union all
    select 'sum(TOTAL_EXPOSURE) = snapshot balance', cast(round(s.bal, 2) as string), cast(round(sum(r.total_exposure), 2) as string)
    from rwa r cross join snapshot s group by s.bal
    having not {{ approx_equal('s.bal', 'sum(r.total_exposure)') }}
    union all
    select 'CAPITAL_ADEQUACY.TOTAL_RWA = sum(RWA)', cast(round(sum(r.rwa), 2) as string), cast(round(max(c.total_rwa), 2) as string)
    from rwa r cross join cap c
    having not {{ approx_equal('sum(r.rwa)', 'max(c.total_rwa)') }}
),

identity as (
    select
        concat('RWA identity ', account_type, '/', customer_segment, '/', risk_weight) as control,
        cast(round(total_exposure * risk_weight, 2) as string) as expected,
        cast(round(rwa, 2) as string) as actual
    from rwa
    where not {{ approx_equal('total_exposure * risk_weight', 'rwa') }}
),

mapping as (
    select
        concat('illegal risk weight branch ', account_type) as control,
        case
            when account_type in ('CHK', 'SAV', 'MMA', 'CD') then '0.00'
            when account_type = 'MTG' then '0.35 or 0.50'
            when account_type = 'HELC' then '0.50'
            when account_type in ('AUTO', 'PERS', 'CC') then '0.75'
            else '1.00'
        end as expected,
        cast(risk_weight as string) as actual
    from rwa
    where not (
        (account_type in ('CHK', 'SAV', 'MMA', 'CD') and risk_weight = 0.00)
        or (account_type = 'MTG' and risk_weight in (0.35, 0.50))
        or (account_type = 'HELC' and risk_weight = 0.50)
        or (account_type in ('AUTO', 'PERS', 'CC') and risk_weight = 0.75)
        or (account_type not in ('CHK', 'SAV', 'MMA', 'CD', 'MTG', 'HELC', 'AUTO', 'PERS', 'CC') and risk_weight = 1.00)
    )
),

status as (
    select 'capital status flags' as control,
        concat_ws('/',
            case when total_rwa = 0 or cet1_ratio >= 4.5 then 'PASS' else 'FAIL' end,
            case when total_rwa = 0 or tier1_ratio >= 6.0 then 'PASS' else 'FAIL' end,
            case when total_rwa = 0 or total_capital_ratio >= 8.0 then 'PASS' else 'FAIL' end) as expected,
        concat_ws('/', cet1_status, tier1_status, total_capital_status) as actual
    from cap
    where cet1_status <> case when total_rwa = 0 or cet1_ratio >= 4.5 then 'PASS' else 'FAIL' end
       or tier1_status <> case when total_rwa = 0 or tier1_ratio >= 6.0 then 'PASS' else 'FAIL' end
       or total_capital_status <> case when total_rwa = 0 or total_capital_ratio >= 8.0 then 'PASS' else 'FAIL' end
       or cet1_capital <> 50000000 or tier1_capital <> 65000000 or total_capital <> 80000000
)

select * from totals
union all
select * from identity
union all
select * from mapping
union all
select * from status
