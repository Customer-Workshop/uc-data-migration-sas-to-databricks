/*
  Mapping parity for the BANKING format catalog (Formats/banking_formats.sas).

  Two directions, both against the seed sas_format_catalog (the SAS value maps):
    1. every *_desc value the models emit equals the catalog label for its code
       (or the OTHER= label when the code is not in the catalog);
    2. every catalog row resolves to its label through the format_* macro,
       including the OTHER= fallback (probed with a code outside the catalog).
*/
with catalog as (
    select format_name, code, label
    from {{ ref('sas_format_catalog') }}
    where format_name in ('$ACCTTYPE', '$ACCTSTAT', '$CUSTSEG', '$REGION', 'RISKRATE', '$TXNCAT')
),

emitted as (
    select '$ACCTTYPE' as fmt, account_type as code, account_type_desc as actual
    from (select distinct account_type, account_type_desc from {{ ref('int_account_metrics') }})
    union all
    select '$ACCTSTAT', account_status, account_status_desc
    from (select distinct account_status, account_status_desc from {{ ref('int_account_metrics') }})
    union all
    select '$CUSTSEG', customer_segment, customer_segment_desc
    from (select distinct customer_segment, customer_segment_desc from {{ ref('int_account_metrics') }})
    union all
    select '$REGION', region_code, region_desc
    from (select distinct region_code, region_desc from {{ ref('int_account_metrics') }})
    union all
    select 'RISKRATE', cast(new_risk_rating as string), new_risk_rating_desc
    from (select distinct new_risk_rating, new_risk_rating_desc from {{ ref('mart_risk_scores') }})
    union all
    select '$TXNCAT', transaction_type, transaction_type_desc
    from (select distinct transaction_type, transaction_type_desc from {{ ref('mart_daily_transactions') }})
),

emitted_check as (
    select
        e.fmt,
        e.code,
        coalesce(c.label, o.label) as expected,
        e.actual
    from emitted e
    left join catalog c
        on e.fmt = c.format_name and e.code = c.code
    left join catalog o
        on e.fmt = o.format_name and o.code = 'OTHER'
),

macro_check as (
    select
        format_name as fmt,
        code,
        label as expected,
        case format_name
            when '$ACCTTYPE' then {{ format_account_type('probe') }}
            when '$ACCTSTAT' then {{ format_account_status('probe') }}
            when '$CUSTSEG' then {{ format_customer_segment('probe') }}
            when '$REGION' then {{ format_region('probe') }}
            when 'RISKRATE' then {{ format_risk_rating('try_cast(probe as int)') }}
            when '$TXNCAT' then {{ format_txn_category('probe') }}
        end as actual
    from (
        -- OTHER= rows are probed with a code that is not in the catalog
        select *, case when code = 'OTHER' then '~~' else code end as probe
        from catalog
    )
)

select fmt, code, expected, actual from emitted_check where not (expected <=> actual)
union all
select fmt, code, expected, actual from macro_check where not (expected <=> actual)
