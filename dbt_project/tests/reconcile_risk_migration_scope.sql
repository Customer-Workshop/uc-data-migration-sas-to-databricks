/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 3 emits exactly the
  scored accounts whose previous rating differs from the new rating or whose
  previous rating is null. It independently checks scope, direction, and the
  source STABLE branch's zero-row consequence.
*/
with scored as (
    select
        s.account_id,
        a.risk_rating as prev_rating,
        case upper(a.risk_rating)
            when 'LOW' then 1
            when 'MEDIUM' then 2
            when 'HIGH' then 3
        end as prev_rank,
        s.risk_rating as curr_rating,
        case
            when s.risk_rating in (1, 2) then 1
            when s.risk_rating in (3, 4, 5) then 2
            when s.risk_rating in (6, 7) then 3
        end as curr_rank
    from {{ ref('mart_risk_scores') }} s
    inner join {{ ref('int_account_metrics') }} a
        on s.account_id = a.account_id
        and a.snapshot_date = current_date()
),

expected as (
    select
        account_id,
        case
            when prev_rating is null then 'NEW'
            when curr_rank < prev_rank then 'UPGRADE'
            when curr_rank > prev_rank then 'DOWNGRADE'
            else 'STABLE'
        end as expected_direction
    from scored
    where prev_rating is null or prev_rank <> curr_rank
),

actual as (
    select
        account_id,
        migration_direction
    from {{ ref('mart_risk_migration') }}
),

scope_mismatches as (
    select
        coalesce(e.account_id, a.account_id) as account_id,
        e.expected_direction,
        a.migration_direction
    from expected e
    full outer join actual a on e.account_id = a.account_id
    where e.account_id is null
       or a.account_id is null
       or e.expected_direction <> a.migration_direction
),

stable_rows as (
    select
        account_id,
        'STABLE' as expected_direction,
        migration_direction
    from actual
    where migration_direction = 'STABLE'
)

select * from scope_mismatches
union all
select * from stable_rows
