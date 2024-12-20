with 

source as (

    select * from {{ source('test', 'orders') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(order_id AS STRING) as order_id,
        CAST(customer_id AS STRING) as customer_id,
        CAST(order_status AS STRING) as order_status,
        CAST(order_purchase_timestamp AS DATETIME) as order_purchase_timestamp,
        CAST(order_approved_at AS DATETIME) as order_approved_at,
        CAST(order_delivered_carrier_date AS DATETIME) as order_delivered_carrier_date,
        CAST(order_delivered_customer_date AS DATETIME) as order_delivered_customer_date,
        CAST(order_estimated_delivery_date AS DATETIME) as order_estimated_delivery_date

    from source

)

select * from renamed
