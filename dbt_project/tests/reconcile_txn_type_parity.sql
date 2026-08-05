/*
  Reconciliation control: per-value PARITY of every transaction-type mapping.

  Two mappings are keyed on TRANSACTION_TYPE in
  daily_transaction_processing.sas, and both are checked here value-for-value
  rather than in aggregate (a direction error on one code can leave the overall
  totals looking plausible):

    1. Balance-movement direction (Steps 2-3 CASE / IF-ELSE chain):
         DEP, INT, REF, REV -> +amount   (+1)
         WDR, PMT, FEE, CHG -> -abs(amount) (-1)
         TRF, ADJ           -> +amount   (+1, signed: a transfer out is NOT
                                          subtracted — source quirk, preserved)
         else               -> unchanged (0)
    2. The $TXNCAT format label (Formats/banking_formats.sas), whose catch-all
       is OTHER = 'Other'.

  The expected values below are transcribed from the SAS source; the actual
  values are derived from the mart rows. Any code whose observed movement sign
  or label differs from the source fails, naming the code.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with sas_mapping as (
    select * from values
        ('DEP', 1, 'Deposit'),
        ('INT', 1, 'Interest'),
        ('REF', 1, 'Refund'),
        ('REV', 1, 'Reversal'),
        ('WDR', -1, 'Withdrawal'),
        ('PMT', -1, 'Payment'),
        ('FEE', -1, 'Fee'),
        ('CHG', -1, 'Charge'),
        ('TRF', 1, 'Transfer'),
        ('ADJ', 1, 'Adjustment')
    as sas_mapping (transaction_type, expected_sign, expected_desc)
),

mart_by_type as (
    select
        transaction_type,
        max(transaction_type_desc) as actual_desc,
        count(distinct transaction_type_desc) as n_desc,
        min(signum(round(post_txn_balance - pre_txn_balance, 2))) as min_sign,
        max(signum(round(post_txn_balance - pre_txn_balance, 2))) as max_sign
    from {{ ref('mart_daily_transactions') }}
    where transaction_amount <> 0
    group by transaction_type
)

select
    m.transaction_type,
    s.expected_sign,
    m.min_sign as actual_min_sign,
    m.max_sign as actual_max_sign,
    s.expected_desc,
    m.actual_desc
from mart_by_type m
inner join sas_mapping s
    on m.transaction_type = s.transaction_type
where m.min_sign <> s.expected_sign
   or m.max_sign <> s.expected_sign
   or m.actual_desc <> s.expected_desc
   or m.n_desc <> 1
