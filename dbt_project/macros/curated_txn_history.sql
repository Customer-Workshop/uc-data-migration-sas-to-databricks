{#
    curated_txn_history.sql

    The SAS program appends each day's batch to CURATED.DAILY_TRANSACTIONS,
    so that table already holds the prior history when the program runs. The
    SAS extract ships that history as `curated_daily_transactions_history`.
    The synthetic seed (RAW_SCHEMA=raw) has no history, which is the
    equivalent of an empty CURATED table on day one.
#}

{% macro curated_txn_history() %}
    {%- if env_var('RAW_SCHEMA', 'raw') == 'raw_sas' -%}
        select
            transaction_id,
            account_id,
            transaction_date,
            transaction_type,
            transaction_amount,
            channel,
            merchant_category,
            description,
            post_date,
            currency_code
        from {{ source('banking_raw', 'curated_daily_transactions_history') }}
    {%- else -%}
        select
            cast(null as string) as transaction_id,
            cast(null as string) as account_id,
            cast(null as date) as transaction_date,
            cast(null as string) as transaction_type,
            cast(null as double) as transaction_amount,
            cast(null as string) as channel,
            cast(null as string) as merchant_category,
            cast(null as string) as description,
            cast(null as date) as post_date,
            cast(null as string) as currency_code
        where 1 = 0
    {%- endif -%}
{% endmacro %}
