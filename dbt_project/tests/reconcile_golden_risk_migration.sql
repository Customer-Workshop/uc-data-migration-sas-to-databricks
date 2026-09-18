/*
  Golden parity: CURATED.RISK_MIGRATION  <->  mart_risk_migration

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_risk_migration, business date 31JAN2024). Keys: account_id.
  Exact columns: prev_rating, curr_rating, migration_direction.
  Tolerance columns: pd, expected_loss (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_risk_migration') }}
),

g as (
    select * from {{ ref('sas_golden_risk_migration') }}
),

compared as (
    select
        coalesce(cast(m.account_id as string), cast(g.account_id as string)) as account_id,
        case
            when m.account_id is null then 'missing_in_dbt'
            when g.account_id is null then 'missing_in_sas'
            when not (m.prev_rating <=> g.prev_rating) then 'prev_rating'
            when not (m.curr_rating <=> g.curr_rating) then 'curr_rating'
            when not (m.migration_direction <=> g.migration_direction) then 'migration_direction'
            when not {{ approx_equal_rel('cast(m.pd as double)', 'cast(g.pd as double)') }} then 'pd'
            when not {{ approx_equal_rel('cast(m.expected_loss as double)', 'cast(g.expected_loss as double)') }} then 'expected_loss'
            else null
        end as mismatch,
        m.prev_rating as dbt_prev_rating,
        g.prev_rating as sas_prev_rating,
        m.curr_rating as dbt_curr_rating,
        g.curr_rating as sas_curr_rating,
        m.migration_direction as dbt_migration_direction,
        g.migration_direction as sas_migration_direction,
        m.pd as dbt_pd,
        g.pd as sas_pd,
        m.expected_loss as dbt_expected_loss,
        g.expected_loss as sas_expected_loss
    from m
    full outer join g
        on m.account_id <=> g.account_id
)

select * from compared
where mismatch is not null
