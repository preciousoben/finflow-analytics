-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: mart_cac_ltv
-- Layer: Gold
-- Purpose: Calculate CAC, LTV, LTV:CAC ratio and payback period
--   by customer, channel and plan
-- Note: CAC is provided as a pre-calculated field per customer from the
--   source system. In production this would be derived from a marketing
--   spend table joined to customer acquisition data.
-- AI prompt used: "Write a dbt mart model that calculates CAC, LTV,
--   LTV:CAC ratio and payback period from the silver layer customers,
--   subscriptions and monthly revenue tables. Include channel and plan
--   level summaries."

with customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

revenue as (
    select * from {{ ref('stg_monthly_revenue') }}
    where mrr_usd is not null
),

-- Average monthly MRR per customer
avg_mrr_per_customer as (
    select
        customer_id,
        round(avg(mrr_usd), 2)              as avg_monthly_mrr,
        sum(mrr_usd)                        as total_revenue_to_date,
        count(distinct revenue_month)       as months_with_revenue,
        min(revenue_month)                  as first_revenue_month,
        max(revenue_month)                  as last_revenue_month
    from revenue
    group by customer_id
),

-- Churn rate by plan
churn_by_plan as (
    select
        plan,
        count(*)                            as total_customers,
        sum(case when is_churned then 1 else 0 end) as churned_customers,
        round(
            sum(case when is_churned then 1 else 0 end)::float
            / nullif(count(*), 0), 4
        )                                   as monthly_churn_rate
    from subscriptions
    group by plan
),

-- Customer level metrics
customer_metrics as (
    select
        c.customer_id,
        c.signup_date,
        c.signup_month,
        c.acquisition_channel,
        c.cac_usd,
        c.country,
        c.company_size,
        c.industry,

        s.plan,
        s.mrr_usd,
        s.status,
        s.is_churned,
        s.months_active,
        s.billing_cycle,

        a.avg_monthly_mrr,
        a.total_revenue_to_date,
        a.months_with_revenue,

        p.monthly_churn_rate,

        -- LTV = avg monthly MRR / monthly churn rate
        case
            when p.monthly_churn_rate > 0
            then round(a.avg_monthly_mrr / p.monthly_churn_rate, 2)
            else null
        end                                 as projected_ltv,

        -- LTV:CAC ratio
        case
            when c.cac_usd > 0 and p.monthly_churn_rate > 0
            then round(
                (a.avg_monthly_mrr / p.monthly_churn_rate) / c.cac_usd
            , 2)
            else null
        end                                 as ltv_cac_ratio,

        -- CAC payback period in months
        case
            when a.avg_monthly_mrr > 0
            then round(c.cac_usd / a.avg_monthly_mrr, 1)
            else null
        end                                 as cac_payback_months,

        -- Health score: LTV:CAC > 3 is healthy, > 5 is excellent
        case
            when (a.avg_monthly_mrr / nullif(p.monthly_churn_rate, 0))
                / nullif(c.cac_usd, 0) >= 5 then 'excellent'
            when (a.avg_monthly_mrr / nullif(p.monthly_churn_rate, 0))
                / nullif(c.cac_usd, 0) >= 3 then 'healthy'
            when (a.avg_monthly_mrr / nullif(p.monthly_churn_rate, 0))
                / nullif(c.cac_usd, 0) >= 1 then 'marginal'
            else 'poor'
        end                                 as unit_economics_health

    from customers c
    left join subscriptions s   on c.customer_id = s.customer_id
    left join avg_mrr_per_customer a on c.customer_id = a.customer_id
    left join churn_by_plan p   on s.plan = p.plan
),

-- Channel level summary
channel_summary as (
    select
        acquisition_channel,
        count(distinct customer_id)         as total_customers,
        round(avg(cac_usd), 2)              as avg_cac,
        round(avg(projected_ltv), 2)        as avg_ltv,
        round(avg(ltv_cac_ratio), 2)        as avg_ltv_cac_ratio,
        round(avg(cac_payback_months), 1)   as avg_payback_months,
        sum(mrr_usd)                        as total_mrr,
        count(case when unit_economics_health = 'excellent' then 1 end) as excellent_count,
        count(case when unit_economics_health = 'healthy' then 1 end)   as healthy_count,
        count(case when unit_economics_health = 'marginal' then 1 end)  as marginal_count,
        count(case when unit_economics_health = 'poor' then 1 end)      as poor_count
    from customer_metrics
    group by acquisition_channel
)

select * from customer_metrics
order by signup_date

