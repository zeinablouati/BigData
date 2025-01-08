with sellers as (

    select * from {{ ref('stg_test__sellers') }}
),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}
),

int_seller_product_performance as (
    select
        s.seller_id,
        count(distinct oi.order_id) as total_orders,
        round(sum(oi.price), 2) as total_revenue
    from sellers s
    left join order_items oi on s.seller_id = oi.seller_id
    group by s.seller_id
)

select * from int_seller_product_performance