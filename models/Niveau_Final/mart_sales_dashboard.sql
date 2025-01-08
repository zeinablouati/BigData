with 

agg_daily_sales as (
    select * 
    from {{ ref('agg_daily_sales') }}
),

-- Seller performance by category
agg_seller_category_performance as (
    select * 
    from {{ ref('agg_seller_category_performance') }}
),

mart_sales_dashboard as (
    select
        ds.sales_date,
        ds.total_orders,
        ds.total_sales_revenue,
        ds.total_freight_cost,
        ds.total_items_sold,

        scp.seller_id,
        scp.product_category_name,
        scp.total_products_sold,
        scp.total_items_sold as category_items_sold,
        scp.total_revenue as category_revenue,
        scp.avg_item_price as category_avg_item_price,
        scp.total_freight_cost as category_freight_cost
    from agg_daily_sales ds
    left join agg_seller_category_performance scp
        on date_trunc(ds.sales_date, day) = date_trunc(current_date, day)
)

select * from mart_sales_dashboard