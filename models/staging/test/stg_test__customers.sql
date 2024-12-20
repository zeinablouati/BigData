with 

source as (

    select * from {{ source('test', 'customers') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(customer_id AS STRING) as customer_id,
        CAST(customer_unique_id AS STRING) as customer_unique_id,
        CAST(customer_zip_code_prefix AS INT64) as customer_zip_code_prefix,
        CAST(customer_city AS STRING) as customer_city,
        CAST(customer_state AS STRING) as customer_state

    from source

)

select * from renamed
