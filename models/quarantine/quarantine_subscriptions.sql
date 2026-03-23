-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: quarantine_subscriptions
-- Layer: Silver
-- Purpose: Capture subscription records that failed data quality checks
-- Issues captured:
--   1. NULL plan values (billing system failure)
--   2. Zero or NULL MRR (missed billing)
--   3. NULL customer_id (orphaned subscription)

with source as (
    select * from {{ source('bronze', 'subscriptions') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by subscription_id
        order by start_date
    ) = 1
),

quarantined as (
    select
        subscription_id                         as row_id,
        'subscriptions'                         as source_table,
        customer_id,
        plan                                    as raw_plan,
        mrr_usd                                 as raw_mrr_usd,
        start_date                              as raw_start_date,
        status,

        -- Classify the issue
        case
            when customer_id is null
                then 'null_customer_id'
            when plan is null
                then 'null_plan'
            when try_to_number(mrr_usd) is null
                then 'null_mrr'
            when try_to_number(mrr_usd) <= 0
                then 'zero_mrr'
            else 'unknown'
        end                                     as issue_type,

        case
            when plan is null
                then 'Plan not assigned - check billing system'
            when try_to_number(mrr_usd) <= 0
                then 'Zero MRR - check billing processor'
            when customer_id is null
                then 'Orphaned subscription - no matching customer'
            else null
        end                                     as investigation_notes,

        current_timestamp()                     as flagged_at,
        'investigate_billing_system'            as recommended_action

    from deduped
    where 
        (
        customer_id is null
        or plan is null
        or try_to_number(mrr_usd) is null
        or try_to_number(mrr_usd) <= 0
        )

        {% if is_incremental() %}
        and current_timestamp() > (select max(flagged_at) from {{ this }})
        {% endif %}
    
)

select * from quarantined

