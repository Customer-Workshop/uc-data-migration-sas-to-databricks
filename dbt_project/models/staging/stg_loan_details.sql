/*
  stg_loan_details.sql
  Migrated from: ORA_DW.LOAN_DETAILS (read by monthly_regulatory_reporting.sas
  Steps 1-3 through LIBNAME ORA_DW).

  Pass-through staging view; the SAS programs read the Oracle table directly.
*/

select
    account_id,
    loan_purpose,
    orig_amount,
    orig_date,
    term_months,
    ltv,
    days_past_due,
    past_due_amount,
    allowance_amt
from {{ source('banking_raw', 'loan_details') }}
