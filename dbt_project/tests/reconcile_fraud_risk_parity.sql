/*
  Reconciliation test: fraud-risk boundaries transcribed from SAS.

  This is deliberately a fixture test. The expected values are literal SAS
  outcomes, while the actual values invoke the model's shared macro. A
  threshold change in the model macro therefore returns rows and fails this
  test; it is not a restatement of the same CASE on both sides.
*/
with boundary_values(fraud_score, expected_fraud_risk) as (
    values
        (49.99, 'LOW'),
        (50.00, 'MEDIUM'),
        (79.99, 'MEDIUM'),
        (80.00, 'HIGH'),
        (cast(null as double), 'LOW')
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
