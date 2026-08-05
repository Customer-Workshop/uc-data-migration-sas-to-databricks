/*
  Reconciliation test: SAS output routing.

  High-risk DENY rows intentionally belong to MANUAL_REVIEW, and fraud alerts
  must be exactly the HIGH subset of the in-scope adjudicated claims.
*/
with register as (
    select * from {{ ref('mart_claims_register') }}
),

review_queue as (
    select claim_id
    from {{ ref('mart_claims_review_queue') }}
),

alerts as (
    select claim_id
    from {{ ref('mart_fraud_alerts') }}
),

expected_review as (
    select claim_id
    from register
    where adjudication_result in ('DENY', 'PEND')
),

expected_alerts as (
    select claim_id
    from register
    where fraud_risk = 'HIGH'
),

review_mismatches as (
    select claim_id from expected_review
    except
    select claim_id from review_queue
    union all
    select claim_id from review_queue
    except
    select claim_id from expected_review
),

alert_mismatches as (
    select claim_id from expected_alerts
    except
    select claim_id from alerts
    union all
    select claim_id from alerts
    except
    select claim_id from expected_alerts
)

select
    'review_queue' as control,
    claim_id
from review_mismatches
union all
select
    'fraud_alerts' as control,
    claim_id
from alert_mismatches
