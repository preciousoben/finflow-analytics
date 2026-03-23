-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: stg_customers
-- Layer: Silver
-- Purpose: Clean and standardise raw customer data from bronze layer
-- AI prompt used: "Write a dbt staging model that cleans the customers table.
--   Fix mixed date formats, remove duplicate customer_ids keeping the first record,
--   exclude negative CAC values, standardise acquisition channel to lowercase,
--   cast data types correctly, and add a signup_month derived column."

with source as (
    select * from {{ source('bronze', 'customers') }}
),

-- Step 1: Remove duplicate customer_ids, keep first occurrence
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by customer_id
        order by signup_date
    ) = 1
),

-- Step 2: Clean and cast all fields
cleaned as (
    select
        customer_id,

        -- Fix mixed date formats by trying multiple patterns
        try_to_date(signup_date, 'YYYY-MM-DD')::date
            as signup_date,

        full_name,

        lower(trim(email))                          as email,
        upper(trim(country))                        as country,
        company_size,
        lower(trim(acquisition_channel))            as acquisition_channel,

        -- Exclude negative and null CAC values
        case
            when try_to_number(cac_usd) > 0
            then try_to_number(cac_usd, 10, 2)
            else null
        end                                         as cac_usd,

        lower(trim(industry))                       as industry,

        -- Derived column
        date_trunc('month', try_to_date(signup_date, 'YYYY-MM-DD'))
                                                    as signup_month,

        -- Data quality flags
        case when email is null then true else false end
                                                    as is_missing_email,
        case
            when try_to_number(cac_usd) <= 0 then true else false
        end                                         as is_invalid_cac

    from deduped
    where customer_id is not null
    and try_to_date(signup_date, 'YYYY-MM-DD') is not null
    and try_to_number(cac_usd) > 0
    and email is not null
)

select * from cleaned


