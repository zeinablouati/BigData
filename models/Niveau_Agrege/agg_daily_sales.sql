with int_order_summary as (
    select * 
    from {{ ref('int_order_summary') }}
),

agg_daily_sales as (
    select
        date(order_purchase_timestamp) as sales_date,
        count(distinct order_id) as total_orders,
        sum(total_price) as total_sales_revenue,
        sum(total_freight) as total_freight_cost,
        sum(total_items) as total_items_sold
    from int_order_summary
    group by date(order_purchase_timestamp)
)

select * from agg_daily_sales