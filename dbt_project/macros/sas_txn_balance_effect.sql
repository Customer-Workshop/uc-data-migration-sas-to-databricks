/*
  Balance effect of a transaction type, from daily_transaction_processing.sas
  (Step 2 POST_TXN_BALANCE and Step 3 RUNNING_BALANCE use the same branches):
    DEP/INT/REF/REV  -> + TRANSACTION_AMOUNT
    WDR/PMT/FEE/CHG  -> - abs(TRANSACTION_AMOUNT)
    TRF/ADJ          -> + TRANSACTION_AMOUNT
    anything else    -> no change (only reachable if validation is bypassed)
*/

{% macro sas_txn_balance_effect(txn_type, amount) -%}
case
    when {{ txn_type }} in ('DEP', 'INT', 'REF', 'REV') then {{ amount }}
    when {{ txn_type }} in ('WDR', 'PMT', 'FEE', 'CHG') then -abs({{ amount }})
    when {{ txn_type }} in ('TRF', 'ADJ') then {{ amount }}
    else 0
end
{%- endmacro %}
