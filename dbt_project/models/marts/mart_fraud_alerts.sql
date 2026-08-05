/*
  mart_fraud_alerts.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Steps 2 and 4)

  SAS conditionally PROC APPENDs FRAUD_ALERTS only when at least one row
  exists. An empty filtered result is a no-op in dbt. SAS joins
  INDICATOR_FLAGS into ALERT_REASON, but that column is absent from the
  Databricks fraud source, so only the score component is retained.
*/

{{
    config(
        materialized='table'
    )
}}

select
    claim_id,
    policy_id,
    claimant_id,
    fraud_score,
    fraud_risk,
    concat(
        'Fraud score: ',
        cast(round(fraud_score, 0) as int)
    ) as alert_reason,
    current_date() as alert_date
from {{ ref('int_claims_adjudication') }}
where fraud_risk = 'HIGH'
