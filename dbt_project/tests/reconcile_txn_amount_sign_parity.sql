/*
  Parity control: the amount-sign CASE, value-for-value against the SAS source.

  daily_transaction_processing.sas moves a balance by transaction type:
      DEP, INT, REF, REV -> + TRANSACTION_AMOUNT       (signed)
      WDR, PMT, FEE, CHG -> - abs(TRANSACTION_AMOUNT)
      TRF, ADJ           -> + TRANSACTION_AMOUNT       (signed)
      else               -> balance unchanged

  The mapping below is transcribed from that CASE, one row per branch, and is
  compared to the delta the model actually applied (post_txn_balance minus
  pre_txn_balance). It catches a branch that is missing, mis-signed, or has
  drifted onto the catch-all even when the overall total still looks right --
  e.g. subtracting a raw negative WDR amount instead of its absolute value.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with sas_mapping as (
    select * from (
        values
            ('DEP', 'signed'),
            ('INT', 'signed'),
            ('REF', 'signed'),
            ('REV', 'signed'),
            ('WDR', 'negative_abs'),
            ('PMT', 'negative_abs'),
            ('FEE', 'negative_abs'),
            ('CHG', 'negative_abs'),
            ('TRF', 'signed'),
            ('ADJ', 'signed')
    ) as m (transaction_type, sas_rule)
),

model_rows as (
    select
        t.transaction_type,
        t.transaction_amount,
        t.post_txn_balance - t.pre_txn_balance as model_delta
    from {{ ref('int_txn_with_balance') }} t
),

compared as (
    select
        r.transaction_type,
        r.transaction_amount,
        r.model_delta,
        case
            when m.sas_rule = 'signed' then r.transaction_amount
            when m.sas_rule = 'negative_abs' then -abs(r.transaction_amount)
            else 0
        end as sas_delta,
        m.sas_rule
    from model_rows r
    left join sas_mapping m
        on r.transaction_type = m.transaction_type
)

select
    transaction_type,
    sas_rule,
    transaction_amount,
    sas_delta as expected_delta,
    model_delta as actual_delta
from compared
where sas_rule is null
   or abs(model_delta - sas_delta) > 0.005
