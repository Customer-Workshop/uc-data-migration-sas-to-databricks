/*
  Reconciliation test: fraud-risk boundaries transcribed from SAS.

  This is deliberately a fixture test. The expected values are literal SAS
  outcomes, while the actual values invoke the model's shared macro. A
  threshold change in the model macro therefore returns rows and fails this
  test; it is not a restatement of the same CASE on both sides.
*/
with boundary_values as (
    select
        cast(49.99 as double) as fraud_score,
        'LOW' as expected_fraud_risk
    union all
    select
        cast(50.00 as double) as fraud_score,
        'MEDIUM' as expected_fraud_risk
    union all
    select
        cast(79.99 as double) as fraud_score,
        'MEDIUM' as expected_fraud_risk
    union all
    select
        cast(80.00 as double) as fraud_score,
        'HIGH' as expected_fraud_risk
    union all
    select
        cast(null as double) as fraud_score,
        'LOW' as expected_fraud_risk
),

actual as (
    select
        fraud_score,
        expected_fraud_risk,
        {{ format_fraud_risk('fraud_score') }} as actual_fraud_risk
    from boundary_values
)

select
    fraud_score,
    expected_fraud_risk,
    actual_fraud_risk
from actual
where expected_fraud_risk <> actual_fraud_risk
