-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: stg_monthly_revenue
-- Layer: Silver
-- Purpose: Clean and standardise raw monthly revenue data from bronze layer
-- AI prompt used: "Write a dbt staging model that cleans the monthly_revenue table.
--   Remove duplicate revenue_ids, exclude zero and null MRR values,
--   fix date formats, standardise plan names to lowercase,
--   and flag future dated records."

with source as (
    select * from {{ source('bronze', 'monthly_revenue') }}
),

-- Step 1: Remove duplicate revenue_ids, keep first occurrence
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by revenue_id
        order by month
    ) = 1
),

-- Step 2: Clean and cast all fields
cleaned as (
    select
        revenue_id,
        subscription_id,
        customer_id,

        -- Fix date format
        try_to_date(month, 'YYYY-MM-DD')::date      as revenue_month,

        -- Exclude zero and null MRR
        case
            when try_to_number(mrr_usd) > 0
            then try_to_number(mrr_usd, 10, 2)
            else null
        end                                         as mrr_usd,

        lower(trim(plan))                           as plan,
        lower(trim(billing_cycle))                  as billing_cycle,

        -- Derived columns
        date_trunc('month', try_to_date(month, 'YYYY-MM-DD'))
                                                    as revenue_month_start,

        year(try_to_date(month, 'YYYY-MM-DD'))      as revenue_year,
        month(try_to_date(month, 'YYYY-MM-DD'))     as revenue_month_num,

        -- Data quality flags
        case
            when try_to_number(mrr_usd) <= 0 or mrr_usd is null
            then true else false
        end                                         as is_zero_mrr,

        case
            when try_to_date(month, 'YYYY-MM-DD') > current_date
            then true else false
        end                                         as is_future_dated

    from deduped
    where revenue_id is not null
      and customer_id is not null
      and subscription_id is not null
      and try_to_date(month, 'YYYY-MM-DD') <= current_date
)

select * from cleaned

