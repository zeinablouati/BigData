with 

source as (

    select * from {{ source('test', 'products') }}

),

categories as (

    select * from {{ source('test', 'product_category_name_translation') }}
),

renamed as (
    select
        CAST(p._line AS INT64) as _line,
        CAST(p._fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(p.product_id AS STRING) as product_id,
        CAST(COALESCE(c.product_category_name_english, p.product_category_name) AS STRING) as product_category_name,
        CAST(p.product_name_lenght AS FLOAT64) as product_name_lenght,
        CAST(p.product_description_lenght AS FLOAT64) as product_description_lenght,
        CAST(p.product_photos_qty AS FLOAT64) as product_photos_qty,
        CAST(p.product_weight_g AS FLOAT64) as product_weight_g,
        CAST(p.product_length_cm AS FLOAT64) as product_length_cm,
        CAST(p.product_height_cm AS FLOAT64) as product_height_cm,
        CAST(p.product_width_cm AS FLOAT64) as product_width_cm
    from source p
    left join categories c
    on p.product_category_name = c.product_category_name

)

select * from renamed