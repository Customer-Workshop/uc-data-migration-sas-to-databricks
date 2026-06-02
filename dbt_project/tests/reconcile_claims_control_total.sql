/*
  Reconciliation test: claims control total (SUM of claimed_amount) ties out.

  A control total is the classic SAS reconciliation: the analyst summed a money
  column on the source and on the output and confirmed they matched to the penny.
  Here the in-scope claimed amount computed directly from the raw sources must
  equal the total claimed amount carried by stg_claims. If the conversion dropped,
  duplicated, or altered any in-scope claim, this sum diverges.

  Scope mirrors reconcile_claims_completeness (Step 1 validation contract).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_total as (
    select coalesce(sum(c.claimed_amount), 0) as total
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    where p.policy_status = 'ACTIVE'
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
),

model_total as (
    select coalesce(sum(claimed_amount), 0) as total
    from {{ ref('stg_claims') }}
)

select
    s.total as source_claimed_amount,
    m.total as model_claimed_amount,
    m.total - s.total as difference
from source_total s
cross join model_total m
where abs(m.total - s.total) > 0.01
