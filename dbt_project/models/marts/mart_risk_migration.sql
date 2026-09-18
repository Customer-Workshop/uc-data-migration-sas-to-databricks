/*
  mart_risk_migration.sql
  Migrated from: Programs/Banking/credit_risk_scoring.sas (Steps 3-4)
  Output contract: CURATED.RISK_MIGRATION (WORK.RISK_MIGRATION appended with FORCE)

  SAS Original:
    WORK.SCORED s INNER JOIN STG_BANK.CUST_ACCOUNTS_DAILY a on ACCOUNT_ID
    where a.SNAPSHOT_DATE = "&score_date"d
      and (a.RISK_RATING ne s.NEW_RISK_RATING or a.RISK_RATING is null)
    MIGRATION_DIRECTION:
      RISK_RATING missing                  -> NEW
      NEW_RISK_RATING < RISK_RATING        -> UPGRADE
      NEW_RISK_RATING > RISK_RATING        -> DOWNGRADE
      otherwise                            -> STABLE  (unreachable given the WHERE)

  dbt Equivalent:
    Same join and CASE; the prior rating is the demographics RISK_RATING
    carried on the snapshot.
*/

{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'score_date'],
        incremental_strategy='merge'
    )
}}

with scored as (
    select * from {{ ref('mart_risk_scores') }}
    where score_date = {{ sas_run_date() }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
    where snapshot_date = {{ sas_run_date() }}
)

select
    {{ sas_run_date() }} as score_date,
    a.account_id,
    a.risk_rating as prev_rating,
    s.new_risk_rating as curr_rating,
    case
        when a.risk_rating is null then 'NEW'
        when s.new_risk_rating < a.risk_rating then 'UPGRADE'
        when s.new_risk_rating > a.risk_rating then 'DOWNGRADE'
        else 'STABLE'
    end as migration_direction,
    s.pd,
    s.expected_loss
from scored s
inner join accounts a
    on s.account_id = a.account_id
where a.risk_rating <> s.new_risk_rating
   or a.risk_rating is null
