-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: quarantine_customers
-- Layer: Silver
-- Purpose: Capture customer records that failed data quality checks
--   so the data team can investigate and fix at the source
-- Issues captured:
--   1. Unparseable signup dates (mixed/invalid formats)
--   2. NULL or negative CAC values
--   3. NULL customer_ids

with source as (
    select * from {{ source('bronze', 'customers') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by customer_id
        order by signup_date
    ) = 1
),

quarantined as (
    select
        customer_id                             as row_id,
        'customers'                             as source_table,
        signup_date                             as raw_signup_date,
        cac_usd                                 as raw_cac_usd,
        email,
        country,
        acquisition_channel,

        -- Classify the issue
        case
            when customer_id is null
                then 'null_customer_id'
            when try_to_date(signup_date, 'YYYY-MM-DD') is null
                then 'unparseable_date'
            when try_to_number(cac_usd) is null
                then 'null_cac'
            when try_to_number(cac_usd) <= 0
                then 'negative_cac'
            when email is null
                then 'null_email'
            else 'unknown'
        end                                     as issue_type,

        -- Keep raw value for investigation
        case
            when try_to_date(signup_date, 'YYYY-MM-DD') is null
                then signup_date
            when try_to_number(cac_usd) <= 0
                then cac_usd
            else null
        end                                     as problematic_raw_value,

        current_timestamp()                     as flagged_at,
        'investigate_source_system'             as recommended_action

    from deduped
    where
        (
            customer_id is null
            or try_to_date(signup_date, 'YYYY-MM-DD') is null
            or try_to_number(cac_usd) is null
            or try_to_number(cac_usd) <= 0
            or email is null
        )

        {% if is_incremental() %}
        and current_timestamp() > (select max(flagged_at) from {{ this }})
        {% endif %}
)

select * from quarantined


