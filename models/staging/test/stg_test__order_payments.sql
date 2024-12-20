with 

source as (

    select * from {{ source('test', 'order_payments') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(order_id AS STRING) as order_id,
        CAST(payment_sequential AS INT64) as payment_sequential,
        CAST(payment_type AS STRING) as payment_type,
        CAST(payment_installments AS INT64) as payment_installments,
        CAST(payment_value AS FLOAT64) as payment_value

    from source

)

select * from renamed