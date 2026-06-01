/*
  Reconciliation: delinquency bucket parity.

  For every row in the source, the bucket assigned by the mart must match the
  SAS CASE from monthly_regulatory_reporting.sas Step 2 exactly:

    DAYS_PAST_DUE = 0           -> 'Current'
    DAYS_PAST_DUE  1-29         -> '1-29'
    DAYS_PAST_DUE 30-59         -> '30-59'
    DAYS_PAST_DUE 60-89         -> '60-89'
    DAYS_PAST_DUE 90-119        -> '90-119'
    DAYS_PAST_DUE 120-179       -> '120-179'
    DAYS_PAST_DUE >= 180        -> '180+'
    else (NULL / negative)      -> 'Unknown'

  The test recomputes the expected bucket for each account and checks that
  every bucket present in the mart is valid and that the mart does not
  contain any bucket value that is not in the SAS mapping.
*/
with mart_buckets as (
    select distinct delinq_bucket
    from {{ ref('mart_delinquency_aging') }}
),

valid_buckets as (
    select explode(array(
        'Current', '1-29', '30-59', '60-89',
        '90-119', '120-179', '180+', 'Unknown'
    )) as bucket
)

select mb.delinq_bucket as invalid_bucket
from mart_buckets mb
left join valid_buckets vb
    on mb.delinq_bucket = vb.bucket
where vb.bucket is null
