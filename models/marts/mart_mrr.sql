-- AI-generated: Claude Sonnet, reviewed and validated by Precious Oben
-- Model: mart_mrr
-- Layer: Gold
-- Purpose: Calculate MRR metrics by month and plan
--   including new MRR, expansion MRR, churned MRR and net new MRR
-- AI prompt used: "Write a dbt mart model that calculates MRR metrics
--   from the silver layer subscriptions and monthly revenue tables.
--   Include total MRR, new MRR, expansion MRR, churned MRR, net new MRR,
--   and active customer counts broken down by month and plan."

with monthly_revenue as (
    select * from {{ ref('stg_monthly_revenue') }}
    where mrr_usd is not null
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

-- MRR by month and plan
mrr_by_month as (
    select
        revenue_month,
        plan,
        count(distinct customer_id)         as active_customers,
        sum(mrr_usd)                        as total_mrr,
        round(avg(mrr_usd), 2)              as avg_mrr_per_customer
    from monthly_revenue
    group by revenue_month, plan
),

-- New MRR: first month a customer appears in revenue
first_revenue_month as (
    select
        customer_id,
        min(revenue_month)                  as first_month
    from monthly_revenue
    group by customer_id
),

new_mrr as (
    select
        r.revenue_month,
        sum(r.mrr_usd)                      as new_mrr,
        count(distinct r.customer_id)       as new_customers
    from monthly_revenue r
    join first_revenue_month f
        on r.customer_id = f.customer_id
        and r.revenue_month = f.first_month
    group by r.revenue_month
),

-- Expansion MRR: customers whose MRR increased vs previous month
expansion_mrr as (
    select
        this_month.revenue_month,
        sum(this_month.mrr_usd - last_month.mrr_usd) as expansion_mrr,
        count(distinct this_month.customer_id)        as expanded_customers
    from monthly_revenue this_month
    join monthly_revenue last_month
        on  this_month.customer_id = last_month.customer_id
        and last_month.revenue_month = dateadd('month', -1, this_month.revenue_month)
    where this_month.mrr_usd > last_month.mrr_usd
    group by this_month.revenue_month
),

-- Churned MRR: customers who had revenue last month but not this month
churned_mrr as (
    select
        dateadd('month', 1, last_month.revenue_month) as revenue_month,
        sum(last_month.mrr_usd) * -1                  as churned_mrr,
        count(distinct last_month.customer_id)         as churned_customers
    from monthly_revenue last_month
    left join monthly_revenue this_month
        on  last_month.customer_id = this_month.customer_id
        and this_month.revenue_month = dateadd('month', 1, last_month.revenue_month)
    where this_month.customer_id is null
    group by last_month.revenue_month
),

-- Total MRR across all plans per month
mrr_total as (
    select
        revenue_month,
        sum(total_mrr)                      as total_mrr,
        sum(active_customers)               as total_active_customers
    from mrr_by_month
    group by revenue_month
),

final as (
    select
        t.revenue_month,
        m.plan,
        m.active_customers,
        m.total_mrr,
        m.avg_mrr_per_customer,
        t.total_mrr                         as total_mrr_all_plans,
        t.total_active_customers,
        coalesce(n.new_mrr, 0)              as new_mrr,
        coalesce(n.new_customers, 0)        as new_customers,
        coalesce(e.expansion_mrr, 0)        as expansion_mrr,
        coalesce(e.expanded_customers, 0)   as expanded_customers,
        coalesce(c.churned_mrr, 0)          as churned_mrr,
        coalesce(c.churned_customers, 0)    as churned_customers,

        -- Net new MRR = new + expansion + churned (churned is negative)
        coalesce(n.new_mrr, 0)
            + coalesce(e.expansion_mrr, 0)
            + coalesce(c.churned_mrr, 0)    as net_new_mrr,

        -- MRR growth rate vs previous month
        round(
            (t.total_mrr - lag(t.total_mrr) over (order by t.revenue_month))
            / nullif(lag(t.total_mrr) over (order by t.revenue_month), 0) * 100
        , 2)                                as mrr_growth_rate_pct,

        -- ARR = MRR * 12
        t.total_mrr * 12                    as arr

    from mrr_by_month m
    join mrr_total t        on m.revenue_month = t.revenue_month
    left join new_mrr n     on m.revenue_month = n.revenue_month
    left join expansion_mrr e on m.revenue_month = e.revenue_month
    left join churned_mrr c on m.revenue_month = c.revenue_month
)

select * from final
order by revenue_month, plan

