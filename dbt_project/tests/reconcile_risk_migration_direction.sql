/*
  Mapping parity for the RISK_MIGRATION direction CASE and its WHERE filter:
  every scored account whose rating changed (or had none) appears once with
  the right direction; STABLE never appears.
*/
with expected as (
    select
        s.account_id,
        case
            when a.risk_rating is null then 'NEW'
            when s.new_risk_rating < a.risk_rating then 'UPGRADE'
            when s.new_risk_rating > a.risk_rating then 'DOWNGRADE'
        end as direction
    from {{ ref('mart_risk_scores') }} s
    inner join {{ ref('int_account_metrics') }} a on s.account_id = a.account_id
    where s.score_date = {{ sas_run_date() }}
      and (a.risk_rating is null or a.risk_rating <> s.new_risk_rating)
),

actual as (
    select account_id, migration_direction as direction
    from {{ ref('mart_risk_migration') }}
    where score_date = {{ sas_run_date() }}
)

select
    coalesce(e.account_id, a.account_id) as account_id,
    e.direction as expected_direction,
    a.direction as actual_direction
from expected e
full outer join actual a on e.account_id = a.account_id
where not (e.direction <=> a.direction)
