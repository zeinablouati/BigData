with 

source as (

    select * from {{ source('test', 'sellers') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(seller_id AS STRING) as seller_id,
        CAST(seller_zip_code_prefix AS INT64) as seller_zip_code_prefix,
        CAST(seller_city AS STRING) as seller_city,
        CAST(seller_state AS STRING) as seller_state

    from source

)

select * from renamed
