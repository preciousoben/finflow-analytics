{{ config(
    materialized='incremental',
    unique_key='row_id',
    on_schema_change='append_new_columns'
) }}

-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: quarantine_transactions
-- Layer: Silver
-- Purpose: Capture transaction records that failed data quality checks
-- Issues captured:
--   1. NULL customer_id (orphaned transactions)
--   2. Negative or zero amounts
--   3. Duplicate transaction IDs

with source as (
    select * from {{ source('bronze', 'transactions') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by transaction_date
    ) = 1
),

quarantined as (
    select
        transaction_id                          as row_id,
        'transactions'                          as source_table,
        customer_id,
        transaction_date                        as raw_transaction_date,
        amount                                  as raw_amount,
        currency,
        payment_method,
        status,
        channel,
        country,

        -- Classify the issue
        case
            when transaction_id is null
                then 'null_transaction_id'
            when customer_id is null
                then 'null_customer_id'
            when try_to_number(amount) is null
                then 'null_amount'
            when try_to_number(amount) <= 0
                then 'negative_or_zero_amount'
            else 'unknown'
        end                                     as issue_type,

        case
            when customer_id is null
                then 'Orphaned transaction - no matching customer, check payment processor logs'
            when try_to_number(amount) <= 0
                then 'Invalid amount - check payment processor for correct value'
            when transaction_id is null
                then 'Missing transaction ID - check payment processor'
            else null
        end                                     as investigation_notes,

        current_timestamp()                     as flagged_at,
        'investigate_payment_processor'         as recommended_action

    from deduped
    where
        transaction_id is null
        or customer_id is null
        or try_to_number(amount) is null
        or try_to_number(amount) <= 0

    {% if is_incremental() %}
        and current_timestamp() > (select max(flagged_at) from {{ this }})
    {% endif %}
)

select * from quarantined