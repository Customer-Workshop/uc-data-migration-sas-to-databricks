/*
  mart_claims_review_queue.sql
  Migrated from: Programs/Insurance/claims_processing.sas (Step 4)

  SAS PROC APPEND writes MANUAL_REVIEW, including DENY rows for high fraud
  risk and PEND rows from the catch-all branch. This is a full dbt table
  rather than an append target.
*/

{{
    config(
        materialized='table'
    )
}}

select *
from {{ ref('mart_claims_register') }}
where routing_target = 'MANUAL_REVIEW'
