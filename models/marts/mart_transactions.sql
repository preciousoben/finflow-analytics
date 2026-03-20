-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: mart_transactions
-- Layer: Gold
-- Purpose: Calculate transaction success rate, failure analysis
--   and payment method performance by month and channel
-- AI prompt used: "Write a dbt mart model that calculates transaction
--   success rates, failure rates, refund rates and revenue metrics
--   from the silver layer transactions table. Break down by month,
--   payment method, channel and country."

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

-- Monthly transaction summary
monthly_summary as (
    select
        transaction_month,
        count(*)                                as total_transactions,
        count(case when is_successful then 1 end)   as successful_transactions,
        count(case when is_failed then 1 end)        as failed_transactions,
        count(case when is_refunded then 1 end)      as refunded_transactions,
        count(case when status = 'pending' then 1 end) as pending_transactions,

        -- Success rate
        round(
            count(case when is_successful then 1 end)::float
            / nullif(count(*), 0) * 100
        , 2)                                    as success_rate_pct,

        -- Failure rate
        round(
            count(case when is_failed then 1 end)::float
            / nullif(count(*), 0) * 100
        , 2)                                    as failure_rate_pct,

        -- Revenue metrics (successful only)
        sum(case when is_successful then amount_usd else 0 end)
                                                as successful_volume_usd,
        round(avg(case when is_successful then amount_usd end), 2)
                                                as avg_transaction_value_usd,
        sum(case when is_successful then fee_usd else 0 end)
                                                as total_fees_usd,
        count(distinct case when is_successful then customer_id end)
                                                as unique_paying_customers

    from transactions
    group by transaction_month
),

-- Payment method breakdown
payment_method_summary as (
    select
        payment_method,
        count(*)                                as total_transactions,
        round(
            count(case when is_successful then 1 end)::float
            / nullif(count(*), 0) * 100
        , 2)                                    as success_rate_pct,
        round(
            count(case when is_failed then 1 end)::float
            / nullif(count(*), 0) * 100
        , 2)                                    as failure_rate_pct,
        sum(case when is_successful then amount_usd else 0 end)
                                                as total_volume_usd,
        round(avg(case when is_successful then amount_usd end), 2)
                                                as avg_transaction_value_usd
    from transactions
    group by payment_method
),

-- Channel breakdown
channel_summary as (
    select
        channel,
        count(*)                                as total_transactions,
        round(
            count(case when is_successful then 1 end)::float
            / nullif(count(*), 0) * 100
        , 2)                                    as success_rate_pct,
        sum(case when is_successful then amount_usd else 0 end)
                                                as total_volume_usd,
        count(distinct customer_id)             as unique_customers
    from transactions
    group by channel
),

-- Final transaction level output with all metrics
final as (
    select
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.transaction_month,
        t.amount_usd,
        t.currency,
        t.payment_method,
        t.status,
        t.is_successful,
        t.is_failed,
        t.is_refunded,
        t.fee_usd,
        t.channel,
        t.country,
        t.description,

        -- Monthly context
        m.total_transactions                    as month_total_transactions,
        m.success_rate_pct                      as month_success_rate_pct,
        m.successful_volume_usd                 as month_successful_volume_usd,

        -- Payment method context
        pm.success_rate_pct                     as payment_method_success_rate_pct,
        pm.total_volume_usd                     as payment_method_total_volume_usd,

        -- Channel context
        ch.success_rate_pct                     as channel_success_rate_pct

    from transactions t
    left join monthly_summary m
        on t.transaction_month = m.transaction_month
    left join payment_method_summary pm
        on t.payment_method = pm.payment_method
    left join channel_summary ch
        on t.channel = ch.channel
)

select * from final
order by transaction_date, transaction_id
