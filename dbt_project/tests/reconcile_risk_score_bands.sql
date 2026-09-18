/*
  Mapping parity for credit_risk_scoring.sas: rating bands, LGD/EAD branches
  and the EL identity hold on every scored row; all lending accounts in the
  snapshot are scored exactly once (completeness).
*/
with scored as (
    select * from {{ ref('mart_risk_scores') }}
    where score_date = {{ sas_run_date() }}
),

row_checks as (
    select
        account_id,
        case
            when new_risk_rating <> case
                when pd < 0.005 then 1 when pd < 0.01 then 2 when pd < 0.03 then 3
                when pd < 0.07 then 4 when pd < 0.15 then 5 when pd < 0.30 then 6 else 7 end
                then 'rating band'
            when pd <= 0 or pd >= 1 then 'pd out of (0,1)'
            when account_type in {{ sas_secured_products() }} and ltv is not null
                and not {{ approx_equal_rel('lgd', 'greatest(0, least(1, (ltv - 0.5) * 0.8))') }}
                then 'lgd secured with ltv'
            when account_type in {{ sas_secured_products() }} and ltv is null and lgd <> 0.40 then 'lgd secured no ltv'
            when account_type = 'CC' and lgd <> 0.75 then 'lgd credit card'
            when account_type not in {{ sas_secured_products() }} and account_type <> 'CC' and lgd <> 0.50 then 'lgd other'
            when account_type in {{ sas_revolving_products() }}
                and not {{ approx_equal('ead', 'current_balance + 0.5 * (credit_limit - current_balance)') }}
                then 'ead revolving'
            when account_type not in {{ sas_revolving_products() }} and not {{ approx_equal('ead', 'current_balance') }}
                then 'ead term'
            when not {{ approx_equal_rel('expected_loss', 'pd * lgd * ead') }} then 'expected loss identity'
            when account_type not in {{ sas_lending_products() }} then 'non-lending product scored'
        end as issue
    from scored
),

completeness as (
    select
        cast(null as string) as account_id,
        concat('scored rows ', s.n, ' <> lending accounts ', a.n) as issue
    from (select count(*) as n from scored) s
    cross join (
        select count(*) as n from {{ ref('int_account_metrics') }}
        where account_type in {{ sas_lending_products() }}
          and snapshot_date = {{ sas_run_date() }}
    ) a
    where s.n <> a.n
)

select * from row_checks where issue is not null
union all
select * from completeness
