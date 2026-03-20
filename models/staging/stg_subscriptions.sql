-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: stg_subscriptions
-- Layer: Silver
-- Purpose: Clean and standardise raw subscription data from bronze layer
-- AI prompt used: "Write a dbt staging model that cleans the subscriptions table.
--   Remove duplicate subscription_ids, exclude zero and null MRR values,
--   standardise plan names to lowercase, fix mixed date formats,
--   cast booleans correctly, and add a months_active derived column."

with source as (
    select * from {{ source('bronze', 'subscriptions') }}
),

-- Step 1: Remove duplicate subscription_ids, keep first occurrence
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by subscription_id
        order by start_date
    ) = 1
),

-- Step 2: Clean and cast all fields
cleaned as (
    select
        subscription_id,
        customer_id,

        lower(trim(plan))                           as plan,

        -- Exclude zero and null MRR
        case
            when try_to_number(mrr_usd) > 0
            then try_to_number(mrr_usd, 10, 2)
            else null
        end                                         as mrr_usd,

        -- Fix mixed date formats
        try_to_date(start_date, 'YYYY-MM-DD')::date as start_date,
        try_to_date(end_date, 'YYYY-MM-DD')::date   as end_date,

        lower(trim(status))                         as status,

        case
            when upper(trim(is_upgraded)) in ('TRUE','1','YES') then true
            else false
        end                                         as is_upgraded,

        lower(trim(upgrade_plan))                   as upgrade_plan,
        try_to_date(upgrade_date, 'YYYY-MM-DD')::date as upgrade_date,

        case
            when try_to_number(upgrade_mrr_usd) > 0
            then try_to_number(upgrade_mrr_usd, 10, 2)
            else null
        end                                         as upgrade_mrr_usd,

        lower(trim(billing_cycle))                  as billing_cycle,

        -- Derived columns
        case
            when try_to_date(end_date, 'YYYY-MM-DD') is not null
            then datediff(
                'month',
                try_to_date(start_date, 'YYYY-MM-DD'),
                try_to_date(end_date, 'YYYY-MM-DD')
            )
            else datediff(
                'month',
                try_to_date(start_date, 'YYYY-MM-DD'),
                current_date
            )
        end                                         as months_active,

        case
            when lower(trim(status)) = 'churned' then true
            else false
        end                                         as is_churned,

        -- Data quality flags
        case
            when try_to_number(mrr_usd) <= 0 or mrr_usd is null
            then true else false
        end                                         as is_zero_mrr,

        case
            when plan is null then true else false
        end                                         as is_missing_plan

    from deduped
    where subscription_id is not null
      and customer_id is not null
      and plan is not null
      and try_to_number(mrr_usd) > 0

)

select * from cleaned
