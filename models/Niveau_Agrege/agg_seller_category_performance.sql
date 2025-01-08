with 

int_seller_product_performance as (
    select * 
    from {{ ref('int_seller_product_performance') }}
),

product_details as (
    select 
        product_id,
        product_category_name
    from {{ ref('stg_test__products') }}
),

agg_seller_category_performance as (
    select
        spp.seller_id,
        pd.product_category_name,
        count(distinct spp.product_id) as total_products_sold,
        sum(spp.total_items_sold) as total_items_sold,
        sum(spp.total_revenue) as total_revenue,
        avg(spp.avg_item_price) as avg_item_price,
        sum(spp.total_freight_cost) as total_freight_cost
    from int_seller_product_performance spp
    left join product_details pd
    on spp.product_id = pd.product_id
    group by spp.seller_id, pd.product_category_name
)

select * from agg_seller_category_performance