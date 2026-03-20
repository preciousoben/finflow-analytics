-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: stg_transactions
-- Layer: Silver
-- Purpose: Clean and standardise raw transaction data from bronze layer
-- AI prompt used: "Write a dbt staging model that cleans the transactions table.
--   Remove duplicate transaction_ids, exclude null and negative amounts,
--   remove future-dated transactions, exclude null statuses,
--   standardise payment method and status to lowercase,
--   cast amounts and dates correctly, and flag failed transactions."

with source as (
    select * from {{ source('bronze', 'transactions') }}
),

-- Step 1: Remove duplicate transaction_ids, keep first occurrence
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by transaction_date
    ) = 1
),

-- Step 2: Clean and cast all fields
cleaned as (
    select
        transaction_id,
        customer_id,

        -- Fix mixed date formats
        try_to_date(transaction_date, 'YYYY-MM-DD')::date
                                                    as transaction_date,

        -- Exclude negative and zero amounts
        case
            when try_to_number(amount) > 0
            then try_to_number(amount, 12, 2)
            else null
        end                                         as amount_usd,

        upper(trim(currency))                       as currency,
        lower(trim(payment_method))                 as payment_method,
        lower(trim(status))                         as status,

        case
            when try_to_number(fee_usd) > 0
            then try_to_number(fee_usd, 10, 2)
            else null
        end                                         as fee_usd,

        lower(trim(channel))                        as channel,
        upper(trim(country))                        as country,
        lower(trim(description))                    as description,

        -- Derived columns
        case
            when lower(trim(status)) = 'success' then true
            else false
        end                                         as is_successful,

        case
            when lower(trim(status)) = 'failed' then true
            else false
        end                                         as is_failed,

        case
            when lower(trim(status)) = 'refunded' then true
            else false
        end                                         as is_refunded,

        date_trunc('month', try_to_date(transaction_date, 'YYYY-MM-DD'))
                                                    as transaction_month,

        -- Data quality flags
        case
            when try_to_number(amount) <= 0 or amount is null
            then true else false
        end                                         as is_invalid_amount,

        case
            when customer_id is null then true else false
        end                                         as is_missing_customer,

        case
            when try_to_date(transaction_date, 'YYYY-MM-DD') > current_date
            then true else false
        end                                         as is_future_dated

    from deduped
    where transaction_id is not null
      and status is not null
      -- Exclude future dated transactions
      and try_to_date(transaction_date, 'YYYY-MM-DD') <= current_date
)

select * from cleaned

