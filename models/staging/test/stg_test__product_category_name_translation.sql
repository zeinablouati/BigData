with 

source as (

    select * from {{ source('test', 'product_category_name_translation') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(product_category_name AS STRING) as product_category_name,
        CAST(product_category_name_english AS STRING) as product_category_name_english

    from source

)

select * from renamed