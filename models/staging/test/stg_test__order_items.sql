with 

source as (

    select * from {{ source('test', 'order_items') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(order_id AS STRING) as order_id,
        CAST(order_item_id AS INT64) as order_item_id,
        CAST(product_id AS STRING) as product_id,
        CAST(seller_id AS STRING) as seller_id,
        CAST(shipping_limit_date AS TIMESTAMP) as shipping_limit_date,
        CAST(price AS FLOAT64) as price,
        CAST(freight_value AS FLOAT64) as freight_value

    from source

)

select * from renamed
