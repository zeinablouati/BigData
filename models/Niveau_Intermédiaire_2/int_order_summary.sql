with stg_orders as (
    select * from {{ref('stg_test__orders')}}
),

stg_oie as (
    select * from {{ref('int_order_items_enriched')}}
),

int_order_summary as (
    select
        oi.order_id,
        oi.customer_id,
        oi.order_status,
        oi.order_purchase_timestamp,
        oi.order_approved_at,
        oi.order_delivered_carrier_date,
        oi.order_delivered_customer_date,
        oi.order_estimated_delivery_date,
        -- Aggregate details from the enriched order items
        count(oie.order_item_id) as total_items,
        sum(oie.price) as total_price,
        sum(oie.freight_value) as total_freight,
        avg(oie.price) as average_item_price,
        max(oie.shipping_limit_date) as max_shipping_limit_date
    from stg_orders oi
    left join stg_oie oie
    on oi.order_id = oie.order_id
    group by 
        oi.order_id, oi.customer_id, oi.order_status, 
        oi.order_purchase_timestamp, oi.order_approved_at, 
        oi.order_delivered_carrier_date, oi.order_delivered_customer_date, 
        oi.order_estimated_delivery_date
)

select * from int_order_summary