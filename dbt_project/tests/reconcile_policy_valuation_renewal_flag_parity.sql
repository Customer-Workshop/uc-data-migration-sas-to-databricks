/*
  Reconciliation test: RENEWAL_DUE_FLAG mapping parity.

  The SAS program (policy_valuation.sas, Step 1) assigns RENEWAL_DUE_FLAG:
      case when EXPIRATION_DATE <= intnx('month', val_date, 3) then 'Y'
           else 'N'
      end

  This parity check recomputes the flag from the model's own expiration_date
  column and compares it to the stored renewal_due_flag.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select
    policy_id,
    expiry_date,
    renewal_due_flag as stored_value,
    case
        when expiry_date <= add_months(current_date(), 3) then 'Y'
        else 'N'
    end as recomputed_value
from {{ ref('int_policy_valuation') }}
where renewal_due_flag <>
    case
        when expiry_date <= add_months(current_date(), 3) then 'Y'
        else 'N'
    end
